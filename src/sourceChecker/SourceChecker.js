const sizeOf = require("buffer-image-size")
const DetectFileType = require("./DetectFileType")
const jsmd5 = require("js-md5")

class NotImplementedError extends Error {
  constructor() {
    this.message = "This method has not been implemented."
    this.name = "NotImplementedError"
  }
}

//
// This code parses binary format of WebM file.
//  recognizes only some important TAGs
//
// Limitation:
//  This programs reads all binary at once in memory (100MB).
//  It is very bad imprementation, but it is still enough for some small WebM file. 
// Refer:
//  http://www.matroska.org/technical/specs/index.html
// Source: https://gist.github.com/mganeko/9ceee931ac5dde298e81
class WebmParser {
  static curWidth = 0
  static curHeight = 0
  static hasWidth = false
  static hasHeight = false

  static parseWebm(buffer) {
    if (!WebmParser.TAG_DICT) WebmParser.setupTagDictionary();
    WebmParser.hasWidth = false;
    WebmParser.hasHeight = false;
    WebmParser.curWidth = -1;
    WebmParser.curHeight = -1;
    WebmParser._internalParseWebm(buffer);
  }

  static _internalParseWebm(buffer, pos = -1, maxPos = -1) {
    let position = pos == -1 ? 0 : pos;
    let maxPosition = maxPos == -1 ? buffer.byteLength : maxPos;

    while (position < maxPosition) {
      // -- TAG --
      let result = WebmParser.scanWebmTag(buffer, position);
      if (!result) {
        break;
      }
      let tagName = WebmParser.TAG_DICT[result.str];
      position += result.size;

      // --- DATA SIZE ---
      result = WebmParser.scanDataSize(buffer, position);
      if (!result) {
        break;
      }
      position += result.size;

      // ---- DATA ----
      if (tagName === 'EBML') {
        WebmParser._internalParseWebm(buffer, position, (position + result.value));
      }
      else if (tagName === 'Tracks') {
        WebmParser._internalParseWebm(buffer, position, (position + result.value));
      }
      else if (tagName === 'TrackEntry') {
        WebmParser._internalParseWebm(buffer, position, (position + result.value));
      }
      else if (tagName === 'Video') {
        WebmParser._internalParseWebm(buffer, position, (position + result.value));
      }
      else if (tagName === 'PixelWidth') {
        WebmParser.curWidth = WebmParser.scanDataValueU(buffer, position, result.value);
        WebmParser.hasWidth = true;
      }
      else if (tagName === 'PixelHeight') {
        WebmParser.curHeight = WebmParser.scanDataValueU(buffer, position, result.value);
        WebmParser.hasHeight = true;
      }
      else if (tagName === 'Segment') {
        WebmParser._internalParseWebm(buffer, position, (position + result.value));
      }
      else if (tagName === 'Cluster') {
        WebmParser._internalParseWebm(buffer, position, (position + result.value));
      }

      if (result.value >= 0) {
        position += result.value;
      }

      if (position == maxPosition) {
        break;
      }
      else if (position > maxPosition) {
        break;
      }
    }

    return false;
  }


  static byteToHex(b) {
    let str = '0' + b.toString(16);
    let len = str.length;
    return str.substring(len - 2).toUpperCase();
  }

  static TAG_DICT


  static setupTagDictionary() {
    // T - Element Type - The form of data the element contains. m: Master, u: unsigned int, i: signed integer, s: string, 8: UTF-8 string, b: binary, f: float, d: date
    WebmParser.TAG_DICT = new Array();

    WebmParser.TAG_DICT['[1A][45][DF][A3]'] = 'EBML'; // EBML 0	[1A][45][DF][A3] m
    WebmParser.TAG_DICT['[42][86]'] = 'EBMLVersion'; //EBMLVersion	1	[42][86] u
    WebmParser.TAG_DICT['[42][F7]'] = 'EBMLReadVersion'; // EBMLReadVersion	1	[42][F7] u
    WebmParser.TAG_DICT['[42][F2]'] = 'EBMLMaxIDLength'; // EBMLMaxIDLength	1	[42][F2] u
    WebmParser.TAG_DICT['[42][F3]'] = 'EBMLMaxSizeLength'; // EBMLMaxSizeLength	1	[42][F3] u
    WebmParser.TAG_DICT['[42][82]'] = 'DocType'; // DocType	1	[42][82] s
    WebmParser.TAG_DICT['[42][87]'] = 'DocTypeVersion'; // DocTypeVersion	1	[42][87] u
    WebmParser.TAG_DICT['[42][85]'] = 'DocTypeReadVersion'; // DocTypeReadVersion	1	[42][85] u

    WebmParser.TAG_DICT['[EC]'] = 'Void'; // Void	g	[EC] b
    WebmParser.TAG_DICT['[BF]'] = 'CRC-32'; // CRC-32	g	[BF] b
    WebmParser.TAG_DICT['[1C][53][BB][6B]'] = 'Cues'; // Cues	1	[1C][53][BB][6B] m

    WebmParser.TAG_DICT['[18][53][80][67]'] = 'Segment';  // Segment	0	[18][53][80][67] m
    WebmParser.TAG_DICT['[11][4D][9B][74]'] = 'SeekHead'; // SeekHead	1	[11][4D][9B][74] m
    WebmParser.TAG_DICT['[4D][BB]'] = 'Seek'; // Seek	2	[4D][BB] m
    WebmParser.TAG_DICT['[53][AB]'] = 'SeekID'; // SeekID	3	[53][AB] b
    WebmParser.TAG_DICT['[53][AC]'] = 'SeekPosition'; // SeekPosition	3	[53][AC] u

    WebmParser.TAG_DICT['[15][49][A9][66]'] = 'Info'; // Info	1	[15][49][A9][66] m 

    WebmParser.TAG_DICT['[16][54][AE][6B]'] = 'Tracks'; // Tracks	1	[16][54][AE][6B] m
    WebmParser.TAG_DICT['[AE]'] = 'TrackEntry'; // TrackEntry	2	[AE] m
    WebmParser.TAG_DICT['[D7]'] = 'TrackNumber'; // TrackNumber	3	[D7] u
    WebmParser.TAG_DICT['[73][C5]'] = 'TrackUID'; // TrackUID	3	[73][C5] u
    WebmParser.TAG_DICT['[83]'] = 'TrackType'; // TrackType	3	[83] u
    WebmParser.TAG_DICT['[23][E3][83]'] = 'DefaultDuration'; // DefaultDuration	3	[23][E3][83] u
    WebmParser.TAG_DICT['[23][31][4F]'] = 'TrackTimecodeScale'; // TrackTimecodeScale	3	[23][31][4F] f
    WebmParser.TAG_DICT['[86]'] = 'CodecID'; // CodecID	3	[86] s
    WebmParser.TAG_DICT['[63][A2]'] = 'CodecPrivate'; // CodecPrivate	3	[63][A2] b
    WebmParser.TAG_DICT['[25][86][88]'] = 'CodecName'; // CodecName	3	[25][86][88] 8
    WebmParser.TAG_DICT['[E0]'] = 'Video'; // Video	3	[E0] m
    WebmParser.TAG_DICT['[B0]'] = 'PixelWidth'; // PixelWidth	4	[B0] u
    WebmParser.TAG_DICT['[BA]'] = 'PixelHeight'; // PixelHeight	4	[BA] u
    WebmParser.TAG_DICT['[23][83][E3]'] = 'FrameRate'; // FrameRate	4	[23][83][E3] f
    WebmParser.TAG_DICT['[E1]'] = 'Audio'; // Audio	3	[E1] m
    WebmParser.TAG_DICT['[B5]'] = 'SamplingFrequency'; // SamplingFrequency	4	[B5] f
    WebmParser.TAG_DICT['[9F]'] = 'Channels'; // Channels	4	[9F] u
    WebmParser.TAG_DICT['[1F][43][B6][75]'] = 'Cluster'; // Cluster	1	[1F][43][B6][75] m
    WebmParser.TAG_DICT['[E7]'] = 'Timecode'; // Timecode	2	[E7] u
    WebmParser.TAG_DICT['[A3]'] = 'SimpleBlock'; // SimpleBlock	2	[A3] b
  }

  static scanWebmTag(buff, pos) {
    let tagSize = 0;
    let followByte;
    let firstByte = buff.readUInt8(pos);
    let firstMask = 0xff;

    if (firstByte & 0x80) {
      tagSize = 1;
    }
    else if (firstByte & 0x40) {
      tagSize = 2;
    }
    else if (firstByte & 0x20) {
      tagSize = 3;
    }
    else if (firstByte & 0x10) {
      tagSize = 4;
    }
    else {
      console.log('ERROR: bad TAG byte');
      return null;
    }

    let decodeRes = WebmParser.decodeBytes(buff, pos, tagSize, firstByte, firstMask);
    return decodeRes;
  }


  static scanDataSize(buff, pos) {
    let dataSizeSize = 0;
    let followByte;
    let firstByte = buff.readUInt8(pos);
    let firstMask;

    if (firstByte & 0x80) {
      dataSizeSize = 1;
      firstMask = 0x7f;
    }
    else if (firstByte & 0x40) {
      dataSizeSize = 2;
      firstMask = 0x3f;
    }
    else if (firstByte & 0x20) {
      dataSizeSize = 3;
      firstMask = 0x1f;
    }
    else if (firstByte & 0x10) {
      dataSizeSize = 4;
      firstMask = 0x0f;
    }
    else if (firstByte & 0x08) {
      dataSizeSize = 5;
      firstMask = 0x07;
    }
    else if (firstByte & 0x04) {
      dataSizeSize = 6;
      firstMask = 0x03;
    }
    else if (firstByte & 0x02) {
      dataSizeSize = 7;
      firstMask = 0x01;
    }
    else if (firstByte & 0x01) {
      dataSizeSize = 8;
      firstMask = 0x00;
    }
    else {
      console.log('ERROR: bad DATA byte');
      return null;
    }

    let decodeRes = WebmParser.decodeBytes(buff, pos, dataSizeSize, firstByte, firstMask);
    return decodeRes;
  }

  static scanDataValueU(buff, pos, size) {
    let uVal = 0;
    let byteVal;
    for (let i = 0; i < size; i++) {
      byteVal = buff.readUInt8(pos + i);
      uVal = (uVal << 8) + byteVal;
    }

    return uVal;
  }

  static scanDataUTF8(buff, pos, size) {
    let sVal = buff.toString('utf8', pos, pos + size);
    return sVal;
  }

  static scanDataFloat(buff, pos, size) {
    if (size === 4) {
      let f = buff.readFloatBE(pos);
      return f;
    }
    else if (size === 8) {
      let df = buff.readDoubleBE(pos);
      return df;
    }
    else {
      console.error('ERROR. Bad Float size=' + size);
      return null;
    }
  }

  static decodeBytes(buff, pos, size, firstByte, firstMask) {
    let value = firstByte & firstMask;
    let str = ('[' + WebmParser.byteToHex(firstByte) + ']');
    let followByte;
    for (let i = 1; i < size; i++) {
      followByte = buff.readUInt8(pos + i);
      str += '[';
      str += WebmParser.byteToHex(followByte);
      str += ']';
      value = (value << 8) + followByte;
    }

    let res = {};
    res.str = str;
    res.size = size;
    res.value = value;

    return res;
  }
}

const puppeteer = require("puppeteer")

class SourceChecker {
  static MIME_TYPE_TO_FILE_EXTENSION = {
    "image/png": "png",
    "image/apng": "png",
    "image/jpeg": "jpg",
    "image/gif": "gif",
    "video/webm": "webm"
  }
  
  constructor(requiresPuppet, requiresPuppetSetup) {
    if (requiresPuppet) {
      this.puppetReady = false

      puppeteer.launch({ headless: true, args: ["--no-sandbox"] }).then((browser) => {
        this.browser = browser
        if (!requiresPuppetSetup) this.puppetReady = true
        else this.puppetSetup()
      })
    }
  }

  async waitForSelectorOrNull(e, selector, ms) {
    try {
      return await e.waitForSelector(selector, { timeout: ms })
    } catch (e) {
      if (e instanceof puppeteer.TimeoutError) return null
      else throw e
    }
  }

  async _processDirectLink(post, source) {
    try {
      let res = await fetch(source)
      let blob = await res.blob()
      let arrayBuffer = await blob.arrayBuffer()

      let md5 = jsmd5(arrayBuffer)

      let dimensions = await this.getDimensions(blob.type, arrayBuffer)

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
      console.error(`Error with: ${source} (${post._id})`)
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

  processPost(post, current) {
    throw new NotImplementedError()
  }

  supportsSource(source) {
    throw new NotImplementedError()
  }

  async getDimensions(type, arrayBuffer) {
    try {
      if (type != "video/webm") {
        const dimensions = sizeOf(Buffer.from(arrayBuffer))
        return { width: dimensions.width, height: dimensions.height }
      } else {
        WebmParser.parseWebm(Buffer.from(arrayBuffer))
        if (WebmParser.hasHeight && WebmParser.hasWidth) {
          return { width: WebmParser.curWidth, height: WebmParser.curHeight }
        }
      }
    } catch (e) {
      console.error(e)
    }

    return { width: -1, height: -1 }
  }

  async getRealFileType(arrayBuffer) {
    try {
      return (await DetectFileType.fromBuffer(arrayBuffer))?.ext
    } catch (e) {
      console.error(e)
      return null
    }
  }

  async dimensionCheck(post, type, arrayBuffer) {
    try {
      if (type != "video/webm") {
        const dimensions = sizeOf(Buffer.from(arrayBuffer))
        return post.width == dimensions.width && post.height == dimensions.height
      } else {
        WebmParser.parseWebm(Buffer.from(arrayBuffer))
        if (WebmParser.hasHeight && WebmParser.hasWidth) {
          return WebmParser.curWidth == post.width && WebmParser.curHeight == post.height
        }
      }
    } catch (e) {
      console.error(e)
    }

    return false
  }
}

module.exports = SourceChecker