const SourceChecker = require("../SourceChecker")
const jsmd5 = require("js-md5")

class SoFurrySourchChecker extends SourceChecker {
  static URL_BASE = "https://www.sofurryfiles.com/std/content?page="

  constructor() {
    super()

    this.SUPPORTED = [/^https?:\/\/.*sofurry\.com\/view\/(\d+).*/]
  }

  supportsSource(source) {
    for (let supported of this.SUPPORTED) {
      if (supported.test(source)) return true
    }

    return false
  }

  async _internalProcessPost(post, source) {
    let data = (/^https?:\/\/.*sofurry\.com\/view\/(\d+).*/).exec(source)

    let id = data[1]

    if (id) {
      try {
        return await this._processDirectLink(post, `${SoFurrySourchChecker.URL_BASE}${id}`)
      } catch (e) {
        console.error(e)
      }
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

module.exports = SoFurrySourchChecker