module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*")
  
  try {
    // Test fetch availability
    const testFetch = typeof fetch !== "undefined"
    
    // Test a simple fetch call
    let fetchTest = null
    if (testFetch) {
      try {
        const response = await fetch("https://api.webflow.com/v2/sites", {
          headers: {
            "Authorization": `Bearer ${process.env.WEBFLOW_API_TOKEN}`,
            "Content-Type": "application/json"
          }
        })
        fetchTest = {
          status: response.status,
          ok: response.ok,
          statusText: response.statusText
        }
      } catch (fetchError) {
        fetchTest = { error: fetchError.message }
      }
    }
    
    return res.status(200).json({
      fetch_available: testFetch,
      fetch_test: fetchTest,
      node_version: process.version
    })
  } catch (error) {
    return res.status(500).json({ error: error.message, stack: error.stack })
  }
}
