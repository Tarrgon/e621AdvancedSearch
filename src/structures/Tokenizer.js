class Tokenizer {
  constructor (raw) {
    this.raw = raw
    this.split = raw.trim().replace(/\s+/g, " ").replace("\n", " ").split("")
    this.done = false
    this.index = 0
  }

  *[Symbol.iterator]() {
    while(!this.done) {
      yield this.consume()
    }
  }

  peek() {
    let token = ""
    for (let i = this.index; i < this.split.length; i++) {
      let t = this.split[i]
      if (t == " ") {
        return token
      } else {
        token += t
      }
    }
  }

  consume() {
    let token = ""
    for (let i = this.index; i < this.split.length; i++) {
      let t = this.split[i]
      if (t == " ") {
        this.index = i + 1
        this.done = this.index >= this.split.length
        return token
      } else if ((t == "-") && token.length == 0) {
        this.index = i + 1
        this.done = this.index >= this.split.length
        return t
      } else {
        token += t
      }
    }

    this.index = this.split.length
    this.done = true

    return token
  }
}

module.exports = Tokenizer