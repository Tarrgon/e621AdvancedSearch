const SourceChecker = require("../SourceChecker")
const jsmd5 = require("js-md5")

class PixivSourceChecker extends SourceChecker {
  constructor() {
    super()

    this.SUPPORTED = [
      new RegExp(".*:\/\/i\.pximg\.net\/.*\.(png|jpg|jpeg|gif).*"),
    ]
  }

  supportsSource(source) {
    for (let supported of this.SUPPORTED) {
      if (supported.test(source)) return true
    }

    return false
  }

  async _internalProcessPost(post, source) {
    try {
      let res = await fetch(source, { headers: { Referer: "https://www.pixiv.net/" } })
      let blob = await res.blob()
      let arrayBuffer = await blob.arrayBuffer()

      let md5 = jsmd5(arrayBuffer)

      return {
        md5Match: md5 == post.md5,
        dimensionMatch: await super.dimensionCheck(post, blob.type, arrayBuffer),
        fileTypeMatch: SourceChecker.MIME_TYPE_TO_FILE_EXTENSION[blob.type] == post.fileType
      }
    } catch (e) {
      console.error(e)
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

module.exports = PixivSourceChecker