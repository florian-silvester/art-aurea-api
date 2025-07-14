module.exports = async function handler(req, res) {
  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*')
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization')
  
  if (req.method === 'OPTIONS') {
    return res.status(200).end()
  }
  
  return res.status(200).json({
    status: 'healthy',
    timestamp: new Date().toISOString(),
    environment: {
      hasWebflowToken: !!process.env.WEBFLOW_API_TOKEN,
      hasSanityToken: !!process.env.SANITY_API_TOKEN
    }
  })
} 