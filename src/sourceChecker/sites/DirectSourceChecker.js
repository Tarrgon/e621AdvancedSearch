const SourceChecker = require("../SourceChecker")
const jsmd5 = require("js-md5")

class DirectSourceChecker extends SourceChecker {
  constructor() {
    super()

    /**
     * TODO:
     * vk.com
     * InkBunny (not direct)
     * Itaku (not direct)
     * Pixiv (not direct)
     */

    this.SUPPORTED = [
      new RegExp(".*:\/\/pbs\.twimg\.com\/media\/.*\.(png|jpg|jpeg).*"),
      new RegExp(".*:\/\/inkbunny\.net\/files\/.*\.(png|jpg|jpeg|gif).*"),
      new RegExp(".*:\/\/d\.furaffinity\.net\/art\/.*\.(png|jpg|jpeg|gif).*"),
      new RegExp(".*:\/\/media\.baraag\.net\/media_attachments\/.*\.(png|jpg|jpeg|gif).*"),
      new RegExp(".*:\/\/artconomy.com\/media\/art\/.*\.(png|jpg|jpeg|gif|webm).*"),
      new RegExp(".*:\/\/images\.artfight\.net\/.*\.(png|jpg|jpeg|gif|webm).*"),
      new RegExp(".*:\/\/cdn.*\.artstation\.com\/p\/assets\/images\/.*\.(png|jpg|jpeg|gif|webm).*"),
      new RegExp(".*:\/\/derpicdn\.net\/img\/(view|download)\/.*\.(png|jpg|jpeg|gif|webm).*"),
      new RegExp(".*:\/\/images-wixmp-.*\.wixmp\.com\/f\/.*\.(png|jpg|jpeg|gif|webm).*"),
      new RegExp(".*:\/\/dl\.dropboxusercontent\.com\/.*\.(png|jpg|jpeg|gif|webm).*"),
      new RegExp(".*:\/\/.*\.cloudfront\.net\/.*\.(png|jpg|jpeg|gif|webm).*"),
      new RegExp(".*:\/\/itaku.ee\/api\/.*\/.*\.(png|jpg|jpeg|gif|webm).*"),
      new RegExp(".*:\/\/cdn\.weasyl\.com\/.*\/submissions\/.*\.(png|jpg|jpeg|gif|webm).*"),
      new RegExp(".*:\/\/uploads\.ungrounded\.net\/.*\.(png|jpg|jpeg|gif|webm).*"),
      new RegExp(".*:\/\/art\.ngfiles\.com\/images\/.*\.(png|jpg|jpeg|gif|webm).*"),
      new RegExp(".*:\/\/.*\.ib\.metapix\.net\/files\/.*\.(png|jpg|jpeg|gif|webm).*"),
      new RegExp(".*:\/\/files\.catbox\.moe\/.*\.(png|jpg|jpeg|gif|webm).*"),
      new RegExp(".*:\/\/i\.imgur\.com\/.*\.(png|jpg|jpeg|gif|webm).*"),
      new RegExp(".*:\/\/.*sofurryfiles\.com\/.*\?page=(\d*).*"),
      new RegExp(".*:\/\/img\.pawoo\.net\/.*\.(png|jpg|jpeg|gif|webm).*"),
      new RegExp(".*:\/\/pawb\.fun\/system\/media_attachments\/files\/.*\.(png|jpg|jpeg|gif|webm).*"),
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
      let res = await fetch(source)
      let blob = await res.blob()
      let arrayBuffer = await blob.arrayBuffer()

      let md5 = jsmd5(arrayBuffer)

      let dimensions = await super.getDimensions(blob.type, arrayBuffer)

      let realFileType = await this.getRealFileType(arrayBuffer)

      if (!realFileType) {
        return {
          unsupported: true,
          md5Match: false,
          dimensionMatch: false,
          fileTypeMatch: false
        }
      }

      return {
        md5Match: md5 == post.md5,
        dimensionMatch: dimensions.width == post.width && dimensions.height == post.height,
        fileTypeMatch: realFileType == post.fileType,
        fileType: realFileType,
        dimensions
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

module.exports = DirectSourceChecker