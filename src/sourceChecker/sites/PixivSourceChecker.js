const SourceChecker = require("../SourceChecker")
const jsmd5 = require("js-md5")
const config = require("../../config.json")

function wait(ms) {
  return new Promise(r => setTimeout(r, ms))
}

class PixivSourceChecker extends SourceChecker {
  constructor() {
    super(true, true)

    this.SUPPORTED = [/.*:\/\/.*pixiv\.net\/.*artworks\/(\d+).*/]
  }

  async login() {
    let page
    try {
      page = await this.browser.newPage()
      await page.goto("https://accounts.pixiv.net/login")
      let usernameInput = await page.waitForSelector("input[type='text']")
      let passwordInput = await page.waitForSelector("input[type='password']")
      let loginButton = await page.waitForSelector("button[disabled]")
      await usernameInput.type(config.pixiv.username)
      await passwordInput.type(config.pixiv.password)
      await loginButton.evaluate(b => b.click())
      await page.waitForNetworkIdle()
    } catch (e) {
      console.error(e)
    } finally {
      await page.close()
    }
  }

  async puppetSetup() {
    await this.login()
    this.puppetReady = true
  }

  supportsSource(source) {
    for (let supported of this.SUPPORTED) {
      if (supported.test(source)) return true
    }

    return false
  }

  async _internalProcessPost(post, source, retried = false) {
    let page
    try {
      page = await this.browser.newPage()
      await page.goto(source)

      await page.waitForNetworkIdle()

      let images = await page.evaluate(() => {
        return Array.from(document.querySelectorAll("a[href*='i.pximg.net']")).map(e => ({ original: e.href, preview: e.firstElementChild.src }))
      })

      if (images.length == 0) {
        if (retried) {
          console.log(`0 images at ${source} (${post._id})`)

          return {
            unknown: true,
            error: true,
            md5Match: false,
            dimensionMatch: false,
            fileTypeMatch: false
          }
        } else {
          await this.login()
          return await this._internalProcessPost(post, source, true)
        }
      }

      let matchData = []

      for (let image of images) {
        for (let [key, src] of Object.entries(image)) {
          let res = await fetch(src, { headers: { Referer: "https://www.pixiv.net/" } })
          let blob = await res.blob()
          let arrayBuffer = await blob.arrayBuffer()

          let md5 = jsmd5(arrayBuffer)

          let dimensions = await super.getDimensions(blob.type, arrayBuffer)

          let realFileType = await this.getRealFileType(arrayBuffer)

          if (!realFileType) {
            console.log(`MISSING REAL FILE TYPE! ON ${post._id} ${source}`)
            return {
              unsupported: true,
              md5Match: false,
              dimensionMatch: false,
              fileTypeMatch: false
            }
          }

          let d = {
            md5Match: md5 == post.md5,
            dimensionMatch: dimensions.width == post.width && dimensions.height == post.height,
            fileTypeMatch: realFileType == post.fileType,
            fileType: realFileType,
            dimensions,
            isPreview: key == "preview"
          }

          d.score = (d.md5Match * 1000) + (d.dimensionMatch * 500) + d.fileTypeMatch

          matchData.push(d)
        }
      }

      if (matchData.length > 0) {
        matchData.sort((a, b) => b.score - a.score)

        return matchData[0]
      }
    } catch (e) {
      console.error(post._id, source)
      console.error(e)
    } finally {
      await page.close()
    }

    console.log(`FELL OUT AT ${post._id} ${source}`)

    return {
      unknown: true,
      error: true,
      md5Match: false,
      dimensionMatch: false,
      fileTypeMatch: false
    }
  }

  async processPost(post, current) {
    while (!this.puppetReady) {
      await wait(500)
    }

    let data = {}
    for (let source of post.sources) {
      if (current?.data?.[source]) continue
      if (this.supportsSource(source)) {
        data[source] = await this._internalProcessPost(post, source)
      }
    }

    return data
  }
}

module.exports = PixivSourceChecker