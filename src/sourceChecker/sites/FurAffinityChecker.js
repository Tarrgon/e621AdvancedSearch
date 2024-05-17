const SourceChecker = require("../SourceChecker")
const jsmd5 = require("js-md5")
const { JSDOM } = require("jsdom")

class FurAffinityChecker extends SourceChecker {
  static URL_BASE = "https://vxfuraffinity.net/view"

  constructor() {
    super()

    this.SUPPORTED = [new RegExp(".*:\/\/.*furaffinity\.net\/view\/(\d*).*")]
  }

  supportsSource(source) {
    for (let supported of this.SUPPORTED) {
      if (supported.test(source)) return true
    }

    return false
  }

  async _processDirectLink(post, source) {
    try {
      let res = await fetch(source)
      let blob = await res.blob()
      let arrayBuffer = await blob.arrayBuffer()

      let md5 = jsmd5(arrayBuffer)

      let dimensions = await super.getDimensions(blob.type, arrayBuffer)

      return {
        md5Match: md5 == post.md5,
        dimensionMatch: dimensions.width == post.width && dimensions.height == post.height,
        fileTypeMatch: SourceChecker.MIME_TYPE_TO_FILE_EXTENSION[blob.type] == post.fileType,
        fileType: SourceChecker.MIME_TYPE_TO_FILE_EXTENSION[blob.type],
        dimensions
      }
    } catch (e) {
      console.error(post.id, source)
      console.error(e)
    }

    return {
      md5Match: false,
      dimensionMatch: false,
      fileTypeMatch: false
    }
  }

  async _internalProcessPost(post, source) {
    let data = (/.*:\/\/.*furaffinity\.net\/view\/(\d*).*/).exec(source)

    let id = data[1]

    if (id) {
      try {
        let res = await fetch(`${FurAffinityChecker.URL_BASE}/${id}`, { redirect: "manual" })
        let html = await res.text()
        let dom = new JSDOM(html)
        let document = dom.window.document

        let href = document.querySelector("meta[property='og:image']")?.getAttribute("content")

        if (href) {
          return await this._processDirectLink(post, href)
        } else {
          return { unknown: true }
        }
      } catch (e) {
        console.error(e)
      }
    }

    return {
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

module.exports = FurAffinityChecker