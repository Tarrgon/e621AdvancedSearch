const express = require("express")
const router = express.Router()

let utils = null

router.get("/bulk", async (req, res) => {
  try {
    let data = await utils.sourceCheckerManager.checkBulk(req.query.ids.split(","), req.query.checkapproved == "true")
    if (!data) return res.sendStatus(500)
    res.json(data)
  } catch(e) {
    console.error(e)
    res.sendStatus(500)
  }
})


router.get("/:id", async (req, res) => {
  try {
    let data = await utils.sourceCheckerManager.checkFor(req.params.id, req.query.checkapproved == "true")
    if (!data) return res.sendStatus(500)
    res.json(data)
  } catch(e) {
    console.error(e)
    res.sendStatus(500)
  }
})
module.exports = (u) => {
  utils = u
  return router
}
