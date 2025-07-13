module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*")
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
  
  if (req.method === "OPTIONS") return res.status(200).end()
  
  try {
    return res.status(200).json({
      success: true,
      message: "API working!",
      env_check: {
        sanity_exists: !!process.env.SANITY_API_TOKEN,
        webflow_exists: !!process.env.WEBFLOW_API_TOKEN
      }
    })
  } catch (error) {
    return res.status(500).json({ error: error.message })
  }
}
