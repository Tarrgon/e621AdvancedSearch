const SourceChecker = require("../SourceChecker")
const jsmd5 = require("js-md5")
const { JSDOM } = require("jsdom")

class DerpibooruSourceChecker extends SourceChecker {
  constructor() {
    super()
    
    this.SUPPORTED = [/^https?:\/\/derpibooru\.org\/images\/(\d+).*/]
  }

  supportsSource(source) {
    for (let supported of this.SUPPORTED) {
      if (supported.test(source)) return true
    }

    return false
  }

  async _internalProcessPost(post, source) {
    try {
      let res = await fetch(source)
      let html = await res.text()
      let dom = new JSDOM(html)
      let document = dom.window.document

      let href = document.querySelector(".fa-download")?.parentElement?.href

      if (href) {
        return await this._processDirectLink(post, href)
      } else {
        return { unknown: true }
      }
    } catch (e) {
      console.error(e)
    }

    return {
      unknown: true,
      error: true,
      md5Match: false,
      dimensionMatch: false,
      fileTypeMatch: false
    }
  }

  async processPost(post, current) {
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

module.exports = DerpibooruSourceChecker