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
      /.*:\/\/pbs\.twimg\.com\/media\/.*\.(png|jpg|jpeg).*/,
      /.*:\/\/pbs\.twimg\.com\/media\/.*\?format=(png|jpg|jpeg).*/,
      /.*:\/\/inkbunny\.net\/files\/.*\.(png|jpg|jpeg|gif).*/,
      /.*:\/\/d\.furaffinity\.net\/art\/.*\.(png|jpg|jpeg|gif).*/,
      /.*:\/\/media\.baraag\.net\/media_attachments\/.*\.(png|jpg|jpeg|gif).*/,
      /.*:\/\/artconomy.com\/media\/art\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /.*:\/\/images\.artfight\.net\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /.*:\/\/cdn.*\.artstation\.com\/p\/assets\/images\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /.*:\/\/derpicdn\.net\/img\/(view|download)\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /.*:\/\/images-wixmp-.*\.wixmp\.com\/f\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /.*:\/\/dl\.dropboxusercontent\.com\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /.*:\/\/.*\.cloudfront\.net\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /.*:\/\/itaku.ee\/api\/.*\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /.*:\/\/cdn\.weasyl\.com\/.*\/submissions\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /.*:\/\/uploads\.ungrounded\.net\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /.*:\/\/art\.ngfiles\.com\/images\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /.*:\/\/.*\.ib\.metapix\.net\/files\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /.*:\/\/files\.catbox\.moe\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /.*:\/\/i\.imgur\.com\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /.*:\/\/.*sofurryfiles\.com\/.*\?page=(\d+).*/,
      /.*:\/\/img\.pawoo\.net\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /.*:\/\/pawb\.fun\/system\/media_attachments\/files\/.*\.(png|jpg|jpeg|gif|webm).*/,
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