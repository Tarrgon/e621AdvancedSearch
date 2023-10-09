const express = require("express")
const router = express.Router()

let utils = null

router.get("/", async (req, res) => {
  let { tags, limit, page } = req.query

  try {
    let result = await utils.performSearch(tags, limit, page)
    return res.json(result)
  } catch (e) {
    return res.status(e.status).send(e.message)
  }
  
})

module.exports = (u) => {
  utils = u
  return router
}
