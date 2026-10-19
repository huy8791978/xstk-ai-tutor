// File: api/gemini.js
// Serverless Proxy bảo mật GEMINI_API_KEY + Quản lý Hàng đợi, Cache 24h, Pool 3 bộ đề & Rate Limit chuẩn Pacific Time

// ── Hạn mức gói free của AI Studio cho Gemini 3.5 Flash Lite: RPM 15 · TPM 250k · RPD 500.
// Ngưỡng dưới đây đặt thấp hơn hạn mức thật để chừa biên an toàn (đồng hồ Google lệch, app khác dùng chung key).
const RPM_CAP = 12;            // số lượt gọi tối đa trong 60 giây, toàn hệ thống
const TPM_CAP = 200_000;       // token (đầu vào + đầu ra ước lượng) tối đa trong 60 giây
const RPD_CAP = 450;           // số lượt gọi tối đa mỗi ngày (reset 0h giờ Thái Bình Dương, khoảng 14-15h giờ Việt Nam)
const USER_DAILY = 30;         // lượt AI tối đa mỗi người mỗi ngày
const IP_DAILY = 90;           // lượt AI tối đa mỗi địa chỉ mạng mỗi ngày (cả lớp có thể dùng chung wifi)
const QUEUE_MAX = 40;          // giây tối đa một yêu cầu chịu xếp hàng trước khi báo bận
const CACHE_TTL = 24 * 3600;   // bộ nhớ chung giữ kết quả 24 giờ (tính bằng giây)
const POOL_SIZE = 3;           // mỗi (loại, quy trình, chủ đề, mức độ) chỉ sinh tối đa 3 bộ đề, người sau dùng lại
const HEAD = { minimal: 0, medium: 6000, high: 12000 }; // chỗ chừa cho token "suy nghĩ" (tính vào max_output_tokens)

// Trả về chuỗi ngày hiện tại theo múi giờ Thái Bình Dương (America/Los_Angeles) để reset đúng 0h PT (14h-15h VN)
function getPacificDateKey() {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Los_Angeles',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).format(new Date());
}

// Bộ nhớ trạng thái toàn cục trên Server
const STATE = {
  pacificDate: getPacificDateKey(),
  rpdCount: 0,
  userDailyMap: new Map(), // userId -> count
  ipDailyMap: new Map(),   // clientIp -> count
  rollingCalls: [],        // [{ ts, tokens }] trong cửa sổ trượt 60 giây
  cacheStore: new Map(),   // cacheKey -> { text, expiresAt }
  poolStore: new Map()     // poolKey "(loại|quy_trình|chủ_đề|mức_độ)" -> [{ text, expiresAt }]
};

function syncDailyResetAndSlidingWindow() {
  const now = Date.now();
  const currentPTDate = getPacificDateKey();

  // Reset bộ đếm ngày lúc 0h giờ Thái Bình Dương (14h-15h giờ Việt Nam)
  if (STATE.pacificDate !== currentPTDate) {
    STATE.pacificDate = currentPTDate;
    STATE.rpdCount = 0;
    STATE.userDailyMap.clear();
    STATE.ipDailyMap.clear();
  }

  // Lọc bỏ các request đã quá 60 giây trong cửa sổ trượt RPM & TPM
  STATE.rollingCalls = STATE.rollingCalls.filter(item => now - item.ts < 60000);

  // Dọn dẹp Cache đã hết hạn 24h
  for (const [k, v] of STATE.cacheStore.entries()) {
    if (now > v.expiresAt) STATE.cacheStore.delete(k);
  }
}

function getCurrentWindowStats() {
  syncDailyResetAndSlidingWindow();
  const rpm = STATE.rollingCalls.length;
  const tpm = STATE.rollingCalls.reduce((sum, item) => sum + item.tokens, 0);
  return { rpm, tpm, rpd: STATE.rpdCount, pacificDate: STATE.pacificDate };
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Hàng đợi thông minh: đợi tối đa QUEUE_MAX (40 giây) nếu RPM hoặc TPM chạm ngưỡng
async function acquireSlotWithQueue(estimatedTokens) {
  const startWait = Date.now();
  const maxWaitMs = QUEUE_MAX * 1000;

  while (true) {
    const { rpm, tpm } = getCurrentWindowStats();
    if (rpm + 1 <= RPM_CAP && tpm + estimatedTokens <= TPM_CAP) {
      return {
        acquired: true,
        waitedSec: Number(((Date.now() - startWait) / 1000).toFixed(1))
      };
    }

    if (Date.now() - startWait >= maxWaitMs) {
      return {
        acquired: false,
        waitedSec: QUEUE_MAX
      };
    }
    await sleep(1200);
  }
}

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method Not Allowed' });
  }

  syncDailyResetAndSlidingWindow();

  const clientIp =
    (req.headers['x-forwarded-for'] || '').split(',')[0].trim() ||
    req.socket?.remoteAddress ||
    'unknown-ip';

  const {
    systemInstruction = '',
    prompt = '',
    inlineData = null,
    userId = 'anonymous-student',
    thinkingLevel = 'minimal', // 'minimal' | 'medium' | 'high'
    baseOutputTokens = 1500,
    cacheKey = null,
    poolMeta = null // { type, flow, topic, level } phục vụ POOL_SIZE = 3
  } = req.body;

  const now = Date.now();
  const ttlMs = CACHE_TTL * 1000;

  // 1. KIỂM TRA BỘ NHỚ CHUNG CACHE 24 GIỜ (Không tốn lượt gọi AI nếu trùng câu hỏi)
  if (cacheKey && STATE.cacheStore.has(cacheKey)) {
    const cached = STATE.cacheStore.get(cacheKey);
    if (now < cached.expiresAt) {
      const stats = getCurrentWindowStats();
      return res.status(200).json({
        text: cached.text,
        source: 'CACHE_24H',
        rateLimit: {
          ...stats,
          userDaily: STATE.userDailyMap.get(userId) || 0,
          ipDaily: STATE.ipDailyMap.get(clientIp) || 0
        }
      });
    }
  }

  // 2. KIỂM TRA POOL_SIZE = 3 CHO CÙNG (LOẠI, QUY TRÌNH, CHỦ ĐỀ, MỨC ĐỘ)
  let computedPoolKey = null;
  if (poolMeta && poolMeta.type && poolMeta.flow && poolMeta.topic && poolMeta.level) {
    computedPoolKey = `${poolMeta.type}|${poolMeta.flow}|${poolMeta.topic}|${poolMeta.level}`.toLowerCase();
    const existingPool = (STATE.poolStore.get(computedPoolKey) || []).filter(item => now < item.expiresAt);
    STATE.poolStore.set(computedPoolKey, existingPool);

    // Nếu đã đủ 3 bộ đề trong Pool, tái sử dụng ngay cho người sau mà không tốn quota
    if (existingPool.length >= POOL_SIZE) {
      const picked = existingPool[Math.floor(Math.random() * existingPool.length)];
      const stats = getCurrentWindowStats();
      return res.status(200).json({
        text: picked.text,
        source: `POOL_REUSE (${existingPool.length}/${POOL_SIZE})`,
        rateLimit: {
          ...stats,
          userDaily: STATE.userDailyMap.get(userId) || 0,
          ipDaily: STATE.ipDailyMap.get(clientIp) || 0
        }
      });
    }
  }

  // 3. KIỂM TRA HẠN MỨC NGÀY: RPD_CAP (450), USER_DAILY (30), IP_DAILY (90)
  const userUsed = STATE.userDailyMap.get(userId) || 0;
  const ipUsed = STATE.ipDailyMap.get(clientIp) || 0;

  if (STATE.rpdCount + 1 > RPD_CAP) {
    return res.status(429).json({
      error: `Hệ thống đã chạm ngưỡng an toàn ${RPD_CAP} lượt/ngày (RPD_CAP). Hạn mức sẽ tự động làm mới lúc 0h giờ Thái Bình Dương (khoảng 14h-15h giờ Việt Nam).`
    });
  }
  if (userUsed + 1 > USER_DAILY) {
    return res.status(429).json({
      error: `Bạn đã sử dụng hết ${USER_DAILY}/${USER_DAILY} lượt AI cá nhân trong ngày (USER_DAILY). Các bài tập có sẵn trong Pool & Cache vẫn hoạt động bình thường.`
    });
  }
  if (ipUsed + 1 > IP_DAILY) {
    return res.status(429).json({
      error: `Mạng Wi-Fi/IP hiện tại đã đạt ngưỡng ${IP_DAILY}/${IP_DAILY} lượt/ngày (IP_DAILY cho lớp học). Vui lòng thử lại sau hoặc dùng kho đề có sẵn.`
    });
  }

  // 4. TÍNH TOÁN TOKEN ĐẦU VÀO + ĐẦU RA + HEAD (THINKING TOKENS)
  const headTokens = HEAD[thinkingLevel] !== undefined ? HEAD[thinkingLevel] : HEAD.minimal;
  const maxOutputTokens = baseOutputTokens + headTokens;
  const inputTokensEst = Math.ceil((systemInstruction.length + prompt.length) / 3.5) + (inlineData ? 1200 : 0);
  const totalEstimatedTokens = inputTokensEst + maxOutputTokens;

  // 5. XẾP HÀNG TỐI ĐA QUEUE_MAX = 40 GIÂY NẾU CHẠM ĐỈNH RPM_CAP (12) HOẶC TPM_CAP (200_000)
  const queueResult = await acquireSlotWithQueue(totalEstimatedTokens);
  if (!queueResult.acquired) {
    return res.status(503).json({
      error: `Hệ thống đang bận xử lý nhiều yêu cầu cùng lúc (đã chờ tối đa ${QUEUE_MAX} giây theo QUEUE_MAX). Vui lòng thử lại sau ít giây.`
    });
  }

  const API_KEY = process.env.GEMINI_API_KEY;
  if (!API_KEY) {
    return res.status(500).json({ error: 'Chưa cấu hình GEMINI_API_KEY trong biến môi trường Server.' });
  }

  const MODEL_NAME = process.env.GEMINI_MODEL || 'gemini-3.5-flash-lite';
  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${MODEL_NAME}:generateContent?key=${API_KEY}`;

  const parts = [{ text: prompt }];
  if (inlineData && inlineData.data && inlineData.mimeType) {
    parts.push({ inline_data: { mime_type: inlineData.mimeType, data: inlineData.data } });
  }

  try {
    // Ghi nhận tiêu thụ trước khi gọi để giữ biên an toàn tuyệt đối
    STATE.rollingCalls.push({ ts: Date.now(), tokens: totalEstimatedTokens });
    STATE.rpdCount += 1;
    STATE.userDailyMap.set(userId, userUsed + 1);
    STATE.ipDailyMap.set(clientIp, ipUsed + 1);

    const requestBody = {
      system_instruction: { parts: [{ text: systemInstruction }] },
      contents: [{ role: 'user', parts }],
      generationConfig: {
        temperature: 0.35,
        maxOutputTokens
      }
    };

    let response = await fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(requestBody)
    });

    if (response.status === 404) {
      const fallbackEndpoint = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-lite:generateContent?key=${API_KEY}`;
      response = await fetch(fallbackEndpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
      });
    }

    const data = await response.json();
    if (!response.ok) {
      return res.status(response.status).json({ error: data.error?.message || 'Lỗi từ Gemini API' });
    }

    const text = data.candidates?.[0]?.content?.parts?.[0]?.text || 'Không có phản hồi.';

    // Lưu vào Cache 24h
    if (cacheKey) {
      STATE.cacheStore.set(cacheKey, { text, expiresAt: Date.now() + ttlMs });
    }

    // Lưu vào Pool (tối đa POOL_SIZE = 3 bộ đề cho mỗi tổ hợp)
    if (computedPoolKey) {
      const currentList = STATE.poolStore.get(computedPoolKey) || [];
      if (currentList.length < POOL_SIZE) {
        currentList.push({ text, expiresAt: Date.now() + ttlMs });
        STATE.poolStore.set(computedPoolKey, currentList);
      }
    }

    const stats = getCurrentWindowStats();
    return res.status(200).json({
      text,
      source: 'LIVE_GEMINI_API',
      waitedSec: queueResult.waitedSec,
      rateLimit: {
        ...stats,
        userDaily: STATE.userDailyMap.get(userId),
        ipDaily: STATE.ipDailyMap.get(clientIp)
      }
    });
  } catch (err) {
    return res.status(500).json({ error: 'Lỗi kết nối máy chủ AI: ' + err.message });
  }
}
