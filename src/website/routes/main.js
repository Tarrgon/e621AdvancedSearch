const express = require("express")
const router = express.Router()

let utils = null

async function handle(req, res) {
  let { query, q, limit, page } = (req.query || {})
  let { searchAfter } = (req.body || {})

  if (limit && !isNaN(limit)) {
    limit = parseInt(limit)

    if (isNaN(limit)) limit = null
    else if (limit <= 0) limit = 1
    else if (limit > 320) limit = 320
  }

  try {
    let result = await utils.performSearch(query ? query : q, limit ? limit : 50, searchAfter ? searchAfter : parseInt(page))
    // console.log(JSON.stringify(result))
    return res.json(result)
  } catch (e) {
    return res.status(e.status).send(e.message)
  }
}

router.get("/", handle)
router.post("/", handle)

module.exports = (u) => {
  utils = u
  return router
}
