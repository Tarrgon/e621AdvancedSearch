const { Client } = require("@elastic/elasticsearch")
const E621Requester = require("./E621Requester.js")
const Tokenizer = require("./Tokenizer.js")
const fs = require("fs")
const csv = require("csv-parse")
const cron = require("node-cron")
const filesizeParser = require('filesize-parser')
const NodeCache = require("node-cache")
const nonExistentTagCache = new NodeCache({ useClones: false, stdTTL: 300, checkperiod: 120 })

const TOKENS_TO_SKIP = ["~", "-"]
const MODIFIERS = {
  NONE: 0,
  OR: 1
}

const SORTABLE_FIELDS = {
  id: "id",
  score: "score",
  favcount: "favoriteCount",
  tagcount: "tagCount",
  commentcount: "commentCount",
  comment_count: "commentCount",
  mpixels: "mpixels",
  filesize: "fileSize",
  landscape: [{ width: "desc" }, { height: "asc" }],
  portrait: [{ height: "desc" }, { width: "asc" }],
  duration: "duration",
  updated: "updatedAt",
  updated_at: "updatedAt"
}

const META_TAGS_TO_FIELD_NAMES = {
  order: "order",
  randseed: "randomSeed",
  user: "uploaderId",
  approver: "approverId",
  id: "id",
  score: "score",
  favcount: "favoriteCount",
  favoritecount: "favoriteCount",
  commentcount: "commentCount",
  comment_count: "commentCount",
  gentags: "general",
  arttags: "artist",
  chartags: "character",
  copytags: "copyright",
  spectags: "species",
  invtags: "invalid",
  lorTags: "lore",
  loretags: "lore",
  metatags: "meta",
  rating: "rating",
  type: "fileType",
  tagcount: "tagCount",
  width: "width",
  height: "height",
  mpixels: "mpixels",
  megapixels: "mpixels",
  ratio: "ratio",
  filesize: "fileSize",
  status: "status",
  date: "createdAt",
  source: "sources",
  ischild: "isChild",
  isparent: "isParent",
  parent: "parentId",
  hassource: "hasSource",
  ratinglocked: "isRatingLocked",
  notelocked: "isNoteLocked",
  md5: "md5",
  duration: "duration",
  inparent: "inParent",
  inchild: "inChild",
  inancestor: "inAncestor",
  indescendant: "inDescendant"
}

const META_TAGS = ["order", "user", "approver", "id", "score", "favcount", "favoritecount", "commentcount", "comment_count", "tagcount",
  "gentags", "arttags", "chartags", "copytags", "spectags", "invtags", "lortags", "loretags", "metatags", "rating", "type",
  "width", "height", "mpixels", "megapixels", "ratio", "filesize", "status", "date", "source", "ischild", "isparent", "parent", "hassource",
  "ratinglocked", "notelocked", "md5", "duration", "inparent", "inchild", "inancestor", "indescendant"
]

const TAG_CATEGORIES_TO_CATEGORY_ID = {
  general: 0,
  artist: 1,
  copyright: 3, // This isn't a mistake
  character: 4,
  species: 5,
  invalid: 6,
  meta: 7,
  lore: 8
}

const META_MATCH_REGEX = new RegExp(META_TAGS.map(t => `(${t}):(.*)`).join("|"))

function wait(ms) {
  return new Promise(r => setTimeout(r, ms))
}

function toFixed(num, fixed) {
  let re = new RegExp("^-?\\d+(?:\.\\d{0," + (fixed || -1) + "})?")
  return parseFloat(num.toString().match(re)[0])
}

class Utilities {
  static singleton

  constructor(elasticSearchClient) {
    /** @type {Client} */
    this.database = elasticSearchClient

    this.requester = new E621Requester(this)

    this.ensureDatabase()

    this.singleton = this
  }

  async ensureDatabase() {
    let needExports = false

    /* THIS WILL DELETE YOUR ENTIRE DATABASE. IT HAS BEEN COMMENTED MULTIPLE TIMES. DON'T ACCIDENTALLY DO THIS
    // // await this.database.indices.delete({ index: "posts" })
    // // await this.database.indices.delete({ index: "tags" })
    // // await this.database.indices.delete({ index: "tagaliases" })
    // // await this.database.indices.delete({ index: "tagimplications" })
    // // await this.database.indices.delete({ index: "hangingrelationships" })
    */

    let postsExists = await this.database.indices.exists({ index: "posts" })
    if (!postsExists) {
      await this.database.indices.create({
        index: "posts", mappings: {
          properties: {
            md5: { type: "keyword" }
          }
        }
      })
      needExports = true
    }

    let tagsExists = await this.database.indices.exists({ index: "tags" })
    if (!tagsExists) {
      await this.database.indices.create({
        index: "tags", mappings: {
          properties: {
            name: { type: "keyword" }
          }
        }
      })
      needExports = true
    }

    let tagAliasesExists = await this.database.indices.exists({ index: "tagaliases" })
    if (!tagAliasesExists) {
      await this.database.indices.create({
        index: "tagaliases", mappings: {
          properties: {
            antecedentName: { type: "keyword" }
          }
        }
      })
      needExports = true
    }

    let tagimplications = await this.database.indices.exists({ index: "tagimplications" })
    if (!tagimplications) {
      await this.database.indices.create({ index: "tagimplications" })
      needExports = true
    }

    let hangingRelationships = await this.database.indices.exists({ index: "hangingrelationships" })
    if (!hangingRelationships) {
      await this.database.indices.create({ index: "hangingrelationships" })
      needExports = true
    }

    // This will forcefully apply database exports tomorrow. Used to rebuild the database without getting too far behind on updates.
    // cron.schedule(`0 ${new Date(36000000).getHours()} * * *`, () => {
    //   this.fetchAndApplyDatabaseExports()
    // })
    // return

    if (needExports) {
      await this.fetchAndApplyDatabaseExports()

      // Every sunday at 5, get new exports. This ensures we didn't miss anything and allows us to update score, favortie count, and comment count.
      // At the current time, this takes between 15-30 minutes. Updates can't be processed while db exports are being processed.
      cron.schedule(`0 ${new Date(36000000).getHours()} * * 0`, () => {
        this.fetchAndApplyDatabaseExports()
      })
    } else {

      // Every sunday at 5, get new exports. This ensures we didn't miss anything and allows us to update score, favortie count, and comment count.
      // At the current time, this takes between 15-30 minutes. Updates can't be processed while db exports are being processed.
      cron.schedule(`0 ${new Date(36000000).getHours()} * * 0`, () => {
        this.fetchAndApplyDatabaseExports()
      })

      this.updateAll()
    }
  }

  async updateAll() {
    try {
      if (this.currentlyUpdating) return

      this.currentlyUpdating = true
      if (this.processingExport) {
        setTimeout(() => {
          this.currentlyUpdating = false
          this.updateAll()
        }, 20000)
        return
      }

      console.log("Beginning update")
      let t = Date.now()
      if (await this.requester.addNewPosts() === false) {
        setTimeout(() => {
          this.currentlyUpdating = false
          this.updateAll()
        }, 20000)
        return
      }

      console.log("New posts added")

      if (await this.requester.applyUpdates() === false) {
        setTimeout(() => {
          this.currentlyUpdating = false
          this.updateAll()
        }, 20000)
        return
      }

      console.log(`${this.requester.updated} updates applied`)
      this.requester.updated = 0

      if (await this.requester.checkForMisses() === false) {
        setTimeout(() => {
          this.currentlyUpdating = false
          this.updateAll()
        }, 20000)
        return
      }

      console.log("Misses fixed")

      if (await this.requester.updateTags() === false) {
        setTimeout(() => {
          this.currentlyUpdating = false
          this.updateAll()
        }, 20000)
        return
      }

      console.log("Tags updated")

      if (await this.requester.updateTagAliases() === false) {
        setTimeout(() => {
          this.currentlyUpdating = false
          this.updateAll()
        }, 20000)
        return
      }

      console.log("Tag aliases updated")

      if (await this.requester.updateTagImplications() === false) {
        setTimeout(() => {
          this.currentlyUpdating = false
          this.updateAll()
        }, 20000)
        return
      }

      console.log("Tag implications updated")

      console.log(`Update complete. Took ${Date.now() - t}ms`)
    } catch (e) {
      let now = Date.now()
      fs.writeFileSync(`./error-${now}.json`, JSON.stringify(e, null, 4))
      console.error(`Update failed full error written to error-${now}.json`)
      console.error(e)
    }

    setTimeout(() => {
      this.currentlyUpdating = false
      this.updateAll()
    }, 5000)
  }

  async fetchAndApplyDatabaseExports() {
    if (this.processingExport) return

    this.processingExport = true
    let date = new Date()

    if (date.getUTCHours() < 7 || (date.getUTCHours() == 8 && date.getUTCMinutes() < 50)) {
      date = new Date(date.getTime() - 86400000)
    }

    let dateString = date.toISOString().split("T")[0]

    console.log(`Starting export processing: ${dateString}`)

    let startTime = Date.now()

    let postExport = await this.requester.getDatabaseExport(`posts-${dateString}.csv.gz`)
    let tagExport = await this.requester.getDatabaseExport(`tags-${dateString}.csv.gz`)
    let tagAliasExport = await this.requester.getDatabaseExport(`tag_aliases-${dateString}.csv.gz`)
    let tagImplicationExport = await this.requester.getDatabaseExport(`tag_implications-${dateString}.csv.gz`)

    this.updateTagCache = {}

    let time = Date.now()
    await this.processTagExport(tagExport)
    tagExport.destroy()
    fs.rmSync(`tags-${dateString}.csv`)
    console.log(`Tag export processed in ${Date.now() - time}ms`)

    await this.database.indices.refresh({ index: "tags" })

    time = Date.now()
    await this.processPostExport(postExport)
    postExport.destroy()
    fs.rmSync(`posts-${dateString}.csv`)
    console.log(`Post export processed in ${Date.now() - time}ms`)

    await this.database.indices.refresh({ index: "posts" })

    time = Date.now()
    await this.updateAllPostRelationships()
    console.log(`Post relationships updated in ${Date.now() - time}ms`)

    await this.database.indices.refresh({ index: "posts" })

    time = Date.now()
    await this.processTagAliasExport(tagAliasExport)
    tagAliasExport.destroy()
    fs.rmSync(`tag_aliases-${dateString}.csv`)
    console.log(`Tag alias export processed in ${Date.now() - time}ms`)

    await this.database.indices.refresh({ index: "tagaliases" })

    time = Date.now()
    await this.processTagImplicationExport(tagImplicationExport)
    tagImplicationExport.destroy()
    fs.rmSync(`tag_implications-${dateString}.csv`)
    console.log(`Tag implication export processed in ${Date.now() - time}ms`)

    await this.database.indices.refresh({ index: "tagimplications" })

    console.log(`Completed database parsing took: ${Date.now() - startTime}ms`)

    this.updateTagCache = null

    this.processingExport = false

    this.updateAll()
  }

  async processTagExport(stream) {
    let date = new Date()

    if (date.getUTCHours() < 7 || (date.getUTCHours() == 7 && date.getUTCMinutes() < 50)) {
      date = new Date(date.getTime() - 86400000)
    }

    date.setUTCHours(7)
    date.setUTCMinutes(44)

    let parser = stream.pipe(csv.parse({ columns: true, trim: true }))

    let update = async (tags) => {
      let now = Date.now()

      console.log("Batching 10000 tag updates")

      let bulk = []

      let cursor = await this.getTagsWithNames(tags.map(tag => tag.name.toString()))

      for (let tag of cursor) {
        let index = tags.findIndex(t => t.name == tag.name)
        if (index == -1) continue

        let newTag = tags.splice(index, 1)[0]

        if (newTag.post_count <= 0) {
          bulk.push({ delete: { _id: newTag.id.toString() } })

          continue
        }

        if (tag.category == newTag.category && tag.name == newTag.name && tag.postCount == newTag.post_count) continue

        if (tag.category != newTag.category) {
          await this.updateTagCategoryEverywhere(newTag, tag.category, newTag.category)
        }

        bulk.push({ update: { _id: tag.id.toString() } })
        bulk.push({ doc: { id: newTag.id, name: newTag.name, category: newTag.category, postCount: newTag.post_count, updatedAt: newTag.date } })
      }

      for (let tag of tags) {
        if (tag.post_count <= 0) continue

        bulk.push({ index: { _id: tag.id.toString() } })
        bulk.push({ id: tag.id, name: tag.name, category: tag.category, postCount: tag.post_count, updatedAt: tag.date })
      }

      if (bulk.length > 0) {
        let res = await this.database.bulk({ index: "tags", operations: bulk })
        if (res.errors) {
          let now = Date.now()
          console.error(`Bulk had errors, written to bulk-error-${now}.json`)
          fs.writeFileSync(`./bulk-error-${now}.json`, JSON.stringify(res, null, 4))
        }
      }
      console.log(`Operation took ${Date.now() - now}ms`)
    }

    let tags = []

    for await (let data of parser) {
      if (tags.length >= 10000) {
        await update(tags)

        tags.length = 0
      }

      data.id = parseInt(data.id)
      data.category = parseInt(data.category)
      data.name = data.name.toString()
      data.post_count = parseInt(data.post_count)
      data.updatedAt = date

      tags.push(data)
    }

    if (tags.length > 0) await update(tags)
  }

  async createPost(id, tags, uploaderId, approverId, createdAt, updatedAt, md5, sources, rating, width, height, duration,
    favoriteCount, score, parentId, children, fileType, fileSize, commentCount, isDeleted, isPending, isFlagged, isRatingLocked,
    isStatusLocked, isNoteLocked) {

    return new Promise((resolve) => {
      this.expandTagsToArray(tags).then(([tags, flatTags]) => {
        resolve({
          id,
          tags,
          flattenedTags: flatTags,
          tagCount: flatTags.length,
          uploaderId: isNaN(uploaderId) ? null : uploaderId,
          approverId: isNaN(approverId) ? null : approverId,
          createdAt: new Date(createdAt),
          updatedAt: new Date(updatedAt),
          md5,
          sources: typeof (sources) == "string" ? sources.trim().split("\n").filter(s => s) : sources.filter(s => s),
          rating,
          width,
          height,
          ratio: toFixed(width / height, 2),
          mpixels: width * height,
          duration: isNaN(duration) ? 0 : duration,
          favoriteCount,
          score,
          parentId: isNaN(parentId) ? null : parentId,
          children,
          fileType,
          fileSize,
          commentCount,
          isDeleted,
          isPending,
          isFlagged,
          isRatingLocked,
          isStatusLocked,
          isNoteLocked
        })
      })
    })
  }

  async expandTagsToArray(tags) {
    return new Promise(async (resolve) => {

      let toReturn = new Array(9).fill(null).map(() => [])
      let flat = []

      if (typeof (tags) == "string") {
        for (let tagName of tags.split(" ")) {
          tagName = tagName.trim()

          let tag = await this.getOrAddTag(tagName)

          if (!tag) {
            console.error(`Unable to get tag: "${tagName}"`)
            continue
          }

          toReturn[tag.category].push(tag.id)
          flat.push(tag.id)
        }
      } else {
        for (let tagNames of Object.values(tags)) {
          for (let tagName of tagNames) {
            tagName = tagName.trim()

            let tag = await this.getOrAddTag(tagName)

            if (!tag) {
              console.error(`Unable to get tag: "${tagName}"`)
              continue
            }

            toReturn[tag.category].push(tag.id)
            flat.push(tag.id)
          }
        }
      }

      return resolve([toReturn, flat])
    })
  }

  // Very simple thing that I didn't want to do because my brain is fried thanks you to this guy (evil semicolons removed):
  // https://stackoverflow.com/questions/6229197/how-to-know-if-two-arrays-have-the-same-values
  arrayCompare(_arr1, _arr2) {
    // .concat() to not mutate arguments
    const arr1 = _arr1.concat().sort()
    const arr2 = _arr2.concat().sort()

    for (let i = 0; i < arr1.length; i++) {
      if (arr1[i] !== arr2[i]) {
        return false
      }
    }

    return true
  }

  isConsideredNewPost(newPost, oldDatabasePost) {
    return newPost.favoriteCount != oldDatabasePost.favoriteCount || newPost.commentCount != oldDatabasePost.commentCount ||
      newPost.score != oldDatabasePost.score || newPost.isDeleted != oldDatabasePost.isDeleted || newPost.isFlagged != oldDatabasePost.isFlagged ||
      newPost.isPending != oldDatabasePost.isPending || newPost.md5 != oldDatabasePost.md5 || newPost.updatedAt.getTime() != new Date(oldDatabasePost.updatedAt).getTime() ||
      !this.arrayCompare(newPost.flattenedTags, oldDatabasePost.flattenedTags)
  }

  async processPostExport(stream) {
    let parser = stream.pipe(csv.parse({ columns: true, trim: true }))

    let update = async (posts) => {
      let now = Date.now()

      console.log("Batching 10000 post updates")

      let bulk = []

      let cursor = await this.getPostsWithIds(posts.map(post => post.id.toString()), ["id", "updatedAt"])

      for (let post of cursor) {
        let index = posts.findIndex(p => p.id == post.id)
        if (index == -1) continue

        let newPost = posts.splice(index, 1)[0]
        if (this.isConsideredNewPost(newPost, post)) continue

        bulk.push({ update: { _id: newPost.id.toString() } })
        bulk.push({ doc: newPost })

      }

      for (let post of posts) {
        bulk.push({ index: { _id: post.id.toString() } })
        bulk.push(post)
      }

      if (bulk.length > 0) {
        let res = await this.database.bulk({ index: "posts", operations: bulk })
        if (res.errors) {
          let now = Date.now()
          console.error(`Bulk had errors, written to bulk-error-${now}.json`)
          fs.writeFileSync(`./bulk-error-${now}.json`, JSON.stringify(res, null, 4))
        }
      }
      console.log(`Operation took ${Date.now() - now}ms`)
    }

    let posts = []

    for await (let data of parser) {
      if (posts.length >= 10000) {
        await update(await Promise.all(posts))

        posts.length = 0
      }

      if (!data.id) continue

      let p = this.createPost(parseInt(data.id), data.tag_string, parseInt(data.uploader_id), parseInt(data.approver_id), data.created_at + "Z", data.updated_at + "Z",
        data.md5, data.source, data.rating, parseInt(data.image_width), parseInt(data.image_height), parseFloat(data.duration), parseInt(data.fav_count),
        parseInt(data.score), parseInt(data.parent_id), [], data.file_ext, parseInt(data.file_size), parseInt(data.comment_count), data.is_deleted == "t",
        data.is_pending == "t", data.is_flagged == "t", data.is_rating_locked == "t", data.is_status_locked == "t", data.is_note_locked == "t")

      posts.push(p)
    }

    if (posts.length > 0) await update(await Promise.all(posts))
  }

  async processTagAliasExport(stream) {
    let date = new Date()

    if (date.getUTCHours() < 7 || (date.getUTCHours() == 7 && date.getUTCMinutes() < 50)) {
      date = new Date(date.getTime() - 86400000)
    }

    date.setUTCHours(7)
    date.setUTCMinutes(44)

    let parser = stream.pipe(csv.parse({ columns: true, trim: true }))

    let update = async (tagAliases) => {
      let now = Date.now()

      console.log("Batching 10000 tag alias updates")

      let bulk = []

      let cursor = await this.getTagAliasesWithIds(tagAliases.map(tagAlias => tagAlias.id.toString()))
      let usedTags = await this.getTagsWithNames(tagAliases.map(alias => alias.consequent_name))

      for (let tagAlias of cursor) {
        let index = tagAliases.findIndex(t => t.id == tagAlias.id)
        if (index == -1) continue

        let newAlias = tagAliases.splice(index, 1)[0]

        if (newAlias.status == "active") {
          let usedTag = usedTags.find(tag => tag.name == newAlias.consequent_name)
          if (usedTag) {
            newAlias.consequentId = usedTag.id
          } else {
            let tag = await this.getOrAddTag(newAlias.consequent_name)

            if (!tag) {
              console.error(`Unable to get tag: "${newAlias.consequent_name}"`)
              continue
            }

            newAlias.consequentId = tag.id
            usedTags.push(tag)
          }

          if (tagAlias.antecedentName == newAlias.antecedent_name && tagAlias.consequentId == newAlias.consequentId) continue

          bulk.push({ update: { _id: newAlias.id.toString() } })
          bulk.push({ doc: { id: newAlias.id, antecedentName: newAlias.antecedent_name, consequentId: newAlias.consequentId, updatedAt: newAlias.updatedAt } })
        } else {
          bulk.push({ delete: { _id: newAlias.id.toString() } })
        }
      }

      for (let tagAlias of tagAliases) {
        if (tagAlias.status == "active") {
          let usedTag = usedTags.find(tag => tag.name == tagAlias.consequent_name)

          if (usedTag) {
            tagAlias.consequentId = usedTag.id
          } else {
            let tag = await this.getOrAddTag(tagAlias.consequent_name)

            if (!tag) {
              console.error(`Unable to get tag: "${tagAlias.consequent_name}"`)
              continue
            }

            tagAlias.consequentId = tag.id
            usedTags.push(tag)
          }

          bulk.push({ index: { _id: tagAlias.id.toString() } })
          bulk.push({ id: tagAlias.id, antecedentName: tagAlias.antecedent_name, consequentId: tagAlias.consequentId, updatedAt: tagAlias.updatedAt })
        }
      }

      if (bulk.length > 0) {
        let res = await this.database.bulk({ index: "tagaliases", operations: bulk })
        if (res.errors) {
          let now = Date.now()
          console.error(`Bulk had errors, written to bulk-error-${now}.json`)
          fs.writeFileSync(`./bulk-error-${now}.json`, JSON.stringify(res, null, 4))
        }
      }
      console.log(`Operation took ${Date.now() - now}ms`)
    }

    let tagAliases = []

    for await (let data of parser) {
      if (tagAliases.length >= 10000) {
        await update(tagAliases)

        tagAliases.length = 0
      }

      data.id = parseInt(data.id)
      data.updatedAt = date

      tagAliases.push(data)
    }

    if (tagAliases.length > 0) await update(tagAliases)
  }

  async processTagImplicationExport(stream) {
    let date = new Date()

    if (date.getUTCHours() < 7 || (date.getUTCHours() == 7 && date.getUTCMinutes() < 50)) {
      date = new Date(date.getTime() - 86400000)
    }

    date.setUTCHours(7)
    date.setUTCMinutes(44)

    let parser = stream.pipe(csv.parse({ columns: true, trim: true }))

    let update = async (tagImplications) => {
      let now = Date.now()

      console.log("Batching 10000 tag implication updates")

      let bulk = []

      let cursor = await this.getTagImplicationsWithIds(tagImplications.map(tagImplication => tagImplication.id.toString()))

      let tagNames = tagImplications.map(tagImplication => tagImplication.consequent_name).concat(tagImplications.map(tagImplication => tagImplication.antecedent_name))

      tagNames = tagNames.filter((a, i, arr) => arr.indexOf(a) == i)

      let usedTags = await this.getTagsWithNames(tagNames)

      for (let tagImplication of cursor) {
        let index = tagImplications.findIndex(t => t.id == tagImplication.id)
        if (index == -1) continue

        let newImplication = tagImplications.splice(index, 1)[0]

        if (newImplication.status == "active") {
          let child = usedTags.find(tag => tag.name == tagImplication.antecedent_name) || await (async () => {
            let [t, isAlias] = await this.getOrAddAliasOrTag(newImplication.antecedent_name)

            if (t) {
              usedTags.push(t)
            }

            return isAlias ? null : t
          })()

          let parent = usedTags.find(tag => tag.name == tagImplication.consequent_name) || await (async () => {
            let [t, isAlias] = await this.getOrAddAliasOrTag(newImplication.consequent_name)

            if (t) {
              usedTags.push(t)
            }

            return isAlias ? null : t
          })()

          if (parent && child) {
            newImplication.antecedentId = child.id
            newImplication.consequentId = parent.id
          } else {
            // if (!child) {
            //   console.error(`Unable to get tag: "${tagImplication.antecedent_name}"`)
            // }

            // if (!parent) {
            //   console.error(`Unable to get tag: "${tagImplication.consequent_name}"`)
            // }

            continue
          }

          if (tagImplication.antecedentId == newImplication.antecedentId && newImplication.consequentId == newImplication.consequentId) continue

          if (newImplication.antecedentId == newImplication.consequentId) continue

          bulk.push({ update: { _id: newImplication.id.toString() } })
          bulk.push({ doc: { id: newImplication.id, antecedentId: newImplication.antecedentId, consequentId: newImplication.consequentId, updatedAt: newImplication.updatedAt } })
        } else {
          bulk.push({ delete: { _id: newImplication.id.toString() } })
        }
      }

      for (let tagImplication of tagImplications) {
        if (tagImplication.status == "active") {
          let child = usedTags.find(tag => tag.name == tagImplication.antecedent_name) || await (async () => {
            let [t, isAlias] = await this.getOrAddAliasOrTag(tagImplication.antecedent_name)

            if (t) {
              usedTags.push(t)
            }

            return isAlias ? null : t
          })()

          let parent = usedTags.find(tag => tag.name == tagImplication.consequent_name) || await (async () => {
            let [t, isAlias] = await this.getOrAddAliasOrTag(tagImplication.consequent_name)

            if (t) {
              usedTags.push(t)
            }

            return isAlias ? null : t
          })()

          if (parent && child) {
            tagImplication.antecedentId = child.id
            tagImplication.consequentId = parent.id
          } else {
            // if (!child) {
            //   console.error(`Unable to get tag: "${tagImplication.antecedent_name}"`)
            // }

            // if (!parent) {
            //   console.error(`Unable to get tag: "${tagImplication.consequent_name}"`)
            // }

            continue
          }

          if (tagImplication.antecedentId == tagImplication.consequentId) continue

          bulk.push({ index: { _id: tagImplication.id.toString() } })
          bulk.push({ id: tagImplication.id, antecedentId: tagImplication.antecedentId, consequentId: tagImplication.consequentId, updatedAt: tagImplication.updatedAt })
        }
      }

      if (bulk.length > 0) {
        let res = await this.database.bulk({ index: "tagimplications", operations: bulk })
        if (res.errors) {
          let now = Date.now()
          console.error(`Bulk had errors, written to bulk-error-${now}.json`)
          fs.writeFileSync(`./bulk-error-${now}.json`, JSON.stringify(res, null, 4))
        }
      }
      console.log(`Operation took ${Date.now() - now}ms`)
    }

    let tagImplications = []

    for await (let data of parser) {
      if (tagImplications.length >= 10000) {
        await update(tagImplications)
      }

      data.id = parseInt(data.id)
      data.updatedAt = date

      tagImplications.push(data)
    }

    if (tagImplications.length > 0) await update(tagImplications)
  }

  async getLatestPostId() {
    return (await this.database.search({
      index: "posts",
      size: 1,
      sort: { id: "desc" },
      query: { match_all: {} }
    })).hits.hits[0]._source.id
  }

  async getPost(id) {
    if (await this.postExists(id)) {
      return (await this.database.get({ index: "posts", id: id.toString() }))._source
    }

    return null
  }

  async postExists(id) {
    return await this.database.exists({ index: "posts", id: id.toString() })
  }

  async getPostsWithIds(ids, source = true) {
    return (await this.database.mget({ index: "posts", ids, _source: source })).docs.filter(d => d.found).map(d => d._source)
  }

  async updateTagCounts(newPost, existingPost) {
    let changes = []

    if (!existingPost) {
      for (let tag of newPost.flattenedTags) {
        changes.push({ tagId: tag, change: 1 })
      }
    } else {
      for (let tag of newPost.flattenedTags) {
        if (!existingPost.flattenedTags.includes(tag)) changes.push({ tagId: tag, change: 1 })
      }

      for (let tag of existingPost.flattenedTags) {
        if (!newPost.flattenedTags.includes(tag)) changes.push({ tagId: tag, change: -1 })
      }
    }

    let bulk = []

    for (let change of changes) {
      bulk.push({ update: { _id: change.tagId.toString() } })
      bulk.push({
        script: {
          lang: "painless",
          source: "ctx._source.postCount += params.change",
          params: {
            change: change.change
          }
        }
      })
    }

    if (bulk.length > 0) {
      let res = await this.database.bulk({ index: "tags", operations: bulk })
      if (res.errors) {
        let now = Date.now()
        console.error(`Bulk had errors, written to bulk-error-${now}.json`)
        fs.writeFileSync(`./bulk-error-${now}.json`, JSON.stringify(res, null, 4))
      }
    }
  }

  async addPost(post) {
    if (!post.id) {
      console.error("Post with no id attempted to be added")
      return
    }

    await this.database.index({
      index: "posts",
      id: post.id.toString(),
      document: post
    })

    await this.updateRelationships(post)

    await this.updateTagCounts(post)
  }

  async updatePost(post) {
    if (!post.id) {
      console.error("Post with no id attempted to be replaced")
      return
    }

    let existingPost = await this.getPost(post.id)

    await this.database.update({ index: "posts", id: post.id.toString(), doc: post })
    await this.updateRelationships(post)

    await this.updateTagCounts(post, existingPost)
  }

  async bulkUpdatePosts(posts) {
    let bulk = []
    for (let post of posts) {
      bulk.push({ update: { _id: post.id.toString() } })
      bulk.push({ doc: post })
    }

    if (bulk.length > 0) {
      let res = await this.database.bulk({ index: "posts", operations: bulk })
      if (res.errors) {
        let now = Date.now()
        console.error(`Bulk had errors, written to bulk-error-${now}.json`)
        fs.writeFileSync(`./bulk-error-${now}.json`, JSON.stringify(res, null, 4))
      }
    }
  }

  async bulkUpdateOrAddPosts(bulkData) {
    let bulk = []
    for (let post of bulkData.update) {
      bulk.push({ update: { _id: post.id.toString() } })
      bulk.push({ doc: post })
    }

    for (let post of bulkData.create) {
      bulk.push({ index: { _id: post.id.toString() } })
      bulk.push(post)
    }

    if (bulk.length > 0) {
      let res = await this.database.bulk({ index: "posts", operations: bulk })
      if (res.errors) {
        let now = Date.now()
        console.error(`Bulk had errors, written to bulk-error-${now}.json`)
        fs.writeFileSync(`./bulk-error-${now}.json`, JSON.stringify(res, null, 4))
      }
    }
  }

  async updateAllPostRelationships() {

    let update = async (posts) => {
      let now = Date.now()

      console.log(`Batching 10000 post relationship updates last id: ${posts[posts.length - 1].id}`)

      let bulk = []

      let cursor = await this.getPostsWithIds(posts.map(post => post.parentId.toString()), ["id"])

      for (let parent of cursor) {
        let index = posts.findIndex(p => p.parentId == parent.id)
        if (index == -1) continue

        let newPost = posts.splice(index, 1)[0]

        if (!newPost.parentId) continue

        bulk.push({ update: { _id: parent.id.toString() } })
        bulk.push({
          script: {
            lang: "painless",
            source: "if (!ctx._source.children.contains(params.childId)) ctx._source.children.add(params.childId)",
            params: {
              childId: parent.id
            }
          }
        })
      }

      for (let post of posts) {
        if (!post.parentId) continue
        console.log(`Couldn't find ${post.parentId}, upserting to hangingrelationships`)
        bulk.push({ update: { _id: post.parentId.toString(), _index: "hangingrelationships" } })
        bulk.push({
          script: {
            lang: "painless",
            source: `
if (ctx.op == "create") {
  ctx._source.children = params.children
}
else if (!ctx._source.children.contains(params.children[0])) ctx._source.children.add(params.children[0])
`,
            params: {
              children: [post.id]
            }
          },
          scripted_upsert: true,
          upsert: {}
        })
      }

      if (bulk.length > 0) {
        let res = await this.database.bulk({ index: "posts", operations: bulk })
        if (res.errors) {
          let now = Date.now()
          console.error(`Bulk had errors, written to bulk-error-${now}.json`)
          fs.writeFileSync(`./bulk-error-${now}.json`, JSON.stringify(res, null, 4))
        }
      }
      console.log(`Operation took ${Date.now() - now}ms`)
    }

    let posts = []

    let pointInTime = await this.database.openPointInTime({ index: "posts", keep_alive: "5m" })

    let res = await this.database.search({
      size: 1024, sort: { id: "desc" },
      pit: {
        id: pointInTime.id,
        keep_alive: "5m"
      },
      query: {
        bool: {
          must: {
            exists: {
              field: "parentId"
            }
          }
        }
      }
    })

    posts = posts.concat(res.hits.hits.map(t => t._source))

    while (res.hits.hits.length > 0) {
      res = await this.database.search({
        size: 1024, sort: { id: "desc" }, search_after: res.hits.hits[res.hits.hits.length - 1].sort,
        pit: {
          id: pointInTime.id,
          keep_alive: "5m"
        }, query: {
          bool: {
            must: {
              exists: {
                field: "parentId"
              }
            }
          }
        }
      })

      posts = posts.concat(res.hits.hits.map(t => t._source))

      if (posts.length >= 10000) {
        await update(posts)

        posts.length = 0
      }
    }

    if (posts.length > 0) await update(posts)

    await this.database.closePointInTime({ id: pointInTime.id })
  }

  async updateRelationships(post) {
    let hangingRelationships = await this.getHangingRelationships(post.id)

    if (hangingRelationships) {
      await this.database.update({
        index: "posts", id: post.id.toString(), script: {
          lang: "painless",
          source: "ctx._source.children.addAll(params.children)",
          params: {
            children: hangingRelationships.children
          }
        }
      })

      await this.deleteHangingRelationships(post.id)
    }

    if (!post.parentId) return

    if (await this.postExists(post.parentId.toString())) {
      await this.database.update({
        index: "posts", id: post.parentId.toString(), script: {
          lang: "painless",
          source: "if (!ctx._source.children.contains(params.childId)) ctx._source.children.add(params.childId)",
          params: {
            childId: post.id
          }
        }
      })
    } else {
      await this.addHangingRelationship(post.id, post.parentId)
    }
  }

  async addHangingRelationship(childId, parentId) {
    await this.database.update({
      index: "hangingrelationships", id: parentId.toString(), scripted_upsert: true,
      script: {
        lang: "painless",
        source: `
if (ctx.op == "create") {
  ctx._source.children = params.children
}
else if (!ctx._source.children.contains(params.children[0])) ctx._source.children.add(params.children[0])
`,
        params: {
          children: [childId]
        }
      },
      upsert: {}
    })
  }

  async getHangingRelationships(parentId) {
    let exists = await this.database.exists({ index: "hangingrelationships", id: parentId.toString() })

    if (exists) {
      return await this.database.get({ index: "hangingrelationships", id: parentId.toString() })._source
    }

    return null
  }

  async deleteHangingRelationships(id) {
    await this.database.delete({ index: "hangingrelationships", id: id.toString() })
  }

  async getTagsWithIds(ids) {
    return (await this.database.mget({ index: "tags", ids })).docs.filter(d => d.found).map(d => d._source)
  }

  async getTagsWithNames(names) {
    let tags = []

    if (this.updateTagCache) {
      for (let i = names.length - 1; i >= 0; i--) {
        if (this.updateTagCache[names[i]]) {
          tags.push(this.updateTagCache[names[i]])
          names.splice(i, 1)
        }
      }
    }

    for (let i = 0; i < names.length / 1024; i++) {
      let theseNames = names.slice(i * 1024, i * 1024 + 1024)

      let dbTags = (await this.database.search({
        index: "tags",
        size: 1024,
        query: {
          bool: {
            should: theseNames.map(name => ({ term: { name } }))
          }
        }
      })).hits.hits.map(t => t._source)

      tags = tags.concat(dbTags)
    }

    if (this.updateTagCache) {
      for (let tag of tags) {
        this.updateTagCache[tag.name] = tag
      }
    }

    return tags
  }

  async getOrAddTag(tagName) {
    if (nonExistentTagCache.get(tagName)) return null
    let tag = await this.getTagByName(tagName)
    if (tag) return tag

    tag = await this.getNewTag(tagName)
    if (tag) return tag

    return null
  }

  async getOrAddTagById(id) {
    if (nonExistentTagCache.get(id.toString())) return null
    let tag = await this.getTag(id)

    if (tag) return tag

    tag = await this.getNewTagById(id)
    if (tag) return tag

    return null
  }

  async getAliasOrTagId(tagName) {
    let alias = await this.getTagAliasByName(tagName)
    if (alias) return alias.consequentId

    let tag = await this.getTagByName(tagName)

    if (tag) return tag.id

    return null
  }

  async getAliasOrTag(tagName) {
    let alias = await this.getTagAliasByName(tagName)
    if (alias) {
      return await this.getTag(alias.consequentId)
    }

    return await this.getTagByName(tagName)
  }

  async getOrAddAliasOrTag(tagName) {
    let alias = await this.getTagAliasByName(tagName)
    if (alias) {
      return [await this.getTag(alias.consequentId), true]
    }

    return [await this.getOrAddTag(tagName), false]
  }

  async getTag(id) {
    if (await this.database.exists({ index: "tags", id: id.toString() })) {
      return (await this.database.get({ index: "tags", id: id.toString() }))._source
    }

    return null
  }

  async getTagByName(name) {
    if (this.updateTagCache && this.updateTagCache[name]) {
      return this.updateTagCache[name]
    }

    let tag = (await this.database.search({
      index: "tags",
      query: {
        term: {
          name
        }
      }
    })).hits.hits[0]?._source

    if (this.updateTagCache && tag) this.updateTagCache[name] = tag

    return tag
  }

  async getNewTag(tagName) {
    if (nonExistentTagCache.get(tagName)) return null
    let tag = await this.requester.getTag(tagName)

    if (tag) {
      await this.addTag(tag)
    } else {
      nonExistentTagCache.set(tagName, true)
    }

    return tag
  }

  async getNewTagById(id) {
    if (nonExistentTagCache.get(id.toString())) return null
    let tag = await this.requester.getTagById(id)

    if (tag) {
      await this.addTag(tag)
    } else {
      nonExistentTagCache.set(id.toString(), true)
    }

    return tag
  }

  async addTag(tag) {
    await this.database.index({ index: "tags", id: tag.id.toString(), document: tag })
    await this.database.indices.refresh({ index: "tags" })
  }

  async updateTagCategoryEverywhere(tag, oldCategory, newCategory) {

    let update = async (posts) => {
      let now = Date.now()

      console.log(`Batching 10000 post tag category updates last id: ${posts[posts.length - 1].id}`)

      let bulk = []

      for (let post of posts) {
        bulk.push({ update: { _id: post.id.toString() } })
        bulk.push({
          script: {
            lang: "painless",
            source: `
if (ctx._source.tags[params.oldCategory].indexOf(params.tagId) != -1) ctx._source.tags[params.oldCategory].remove(ctx._source.tags[params.oldCategory].indexOf(params.tagId));
if (ctx._source.tags[params.newCategory].indexOf(params.tagId) == -1) ctx._source.tags[params.newCategory].add(params.tagId);
`,
            params: {
              oldCategory,
              newCategory,
              tagId: tag.id
            }
          },
        })
      }

      if (bulk.length > 0) {
        let res = await this.database.bulk({ index: "posts", operations: bulk })
        if (res.errors) {
          let now = Date.now()
          console.error(`Bulk had errors, written to bulk-error-${now}.json`)
          fs.writeFileSync(`./bulk-error-${now}.json`, JSON.stringify(res, null, 4))
        }
      }
      console.log(`Operation took ${Date.now() - now}ms`)
    }

    let posts = []

    let pointInTime = await this.database.openPointInTime({ index: "posts", keep_alive: "5m" })

    let res = await this.database.search({
      size: 1024, sort: { id: "desc" },
      pit: {
        id: pointInTime.id,
        keep_alive: "5m"
      },
      query: {
        bool: {
          must: {
            term: {
              flattenedTags: tag.id
            }
          }
        }
      }
    })

    posts = posts.concat(res.hits.hits.map(t => t._source))

    while (res.hits.hits.length > 0) {
      res = await this.database.search({
        size: 1024, sort: { id: "desc" }, search_after: res.hits.hits[res.hits.hits.length - 1].sort,
        pit: {
          id: pointInTime.id,
          keep_alive: "5m"
        }, query: {
          bool: {
            must: {
              term: {
                flattenedTags: tag.id
              }
            }
          }
        }
      })

      posts = posts.concat(res.hits.hits.map(t => t._source))

      if (posts.length >= 10000) {
        await update(posts)

        posts.length = 0
      }
    }

    if (posts.length > 0) await update(posts)

    await this.database.closePointInTime({ id: pointInTime.id })
  }

  async fixDoubles() {

    let update = async (posts) => {
      let now = Date.now()

      console.log(`Batching 10000 post tag duplicate fixes last id: ${posts[posts.length - 1].id}`)

      let bulk = []

      for (let post of posts) {
        bulk.push({ update: { _id: post.id.toString() } })
        bulk.push({
          script: {
            lang: "painless",
            source: `
for (int i = 0; i < ctx._source.tags.size(); i++) {
  Set set = new HashSet(ctx._source.tags[i]);
  ctx._source.tags[i].clear();
  ctx._source.tags[i].addAll(set);
}
`,
          },
        })
      }

      if (bulk.length > 0) {
        let res = await this.database.bulk({ index: "posts", operations: bulk })
        if (res.errors) {
          let now = Date.now()
          console.error(`Bulk had errors, written to bulk-error-${now}.json`)
          fs.writeFileSync(`./bulk-error-${now}.json`, JSON.stringify(res, null, 4))
        }
      }
      console.log(`Operation took ${Date.now() - now}ms`)
    }

    let posts = []

    let pointInTime = await this.database.openPointInTime({ index: "posts", keep_alive: "5m" })

    let res = await this.database.search({
      size: 1024, sort: { id: "desc" },
      pit: {
        id: pointInTime.id,
        keep_alive: "5m"
      },
      query: {
        match_all: {}
      }
    })

    posts = posts.concat(res.hits.hits.map(t => t._source))

    while (res.hits.hits.length > 0) {
      res = await this.database.search({
        size: 1024, sort: { id: "desc" }, search_after: res.hits.hits[res.hits.hits.length - 1].sort,
        pit: {
          id: pointInTime.id,
          keep_alive: "5m"
        }, query: {
          match_all: {}
        }
      })

      posts = posts.concat(res.hits.hits.map(t => t._source))

      if (posts.length >= 10000) {
        await update(posts)

        posts.length = 0
      }
    }

    if (posts.length > 0) await update(posts)

    await this.database.closePointInTime({ id: pointInTime.id })
  }

  async updateTag(tag) {
    let existingTag = await this.getTag(tag.id)

    if (existingTag.category != tag.category) {
      await this.updateTagCategoryEverywhere(tag, existingTag.category, tag.category)
    }

    await this.database.update({ index: "tags", id: tag.id.toString(), doc: tag })
    await this.database.indices.refresh({ index: "tags" })
  }

  async deleteTag(id) {
    await this.database.delete({ index: "tags", id: id.toString() })
    await this.database.indices.refresh({ index: "tags" })
  }

  async getLatestTagAliasId() {
    return (await this.database.search({
      index: "tagaliases",
      size: 1,
      sort: { id: "desc" },
      query: { match_all: {} }
    })).hits.hits[0]._source.id
  }

  async getTagAlias(id) {
    if (await this.database.exists({ index: "tagaliases", id: id.toString() })) {
      return (await this.database.get({ index: "tagaliases", id: id.toString() }))._source
    }

    return null
  }

  async getTagAliasesWithIds(ids) {
    return (await this.database.mget({ index: "tagaliases", ids })).docs.filter(d => d.found).map(d => d._source)
  }

  async getTagAliasByName(tagName) {
    return (await this.database.search({
      index: "tagaliases",
      query: {
        term: {
          antecedentName: tagName
        }
      }
    })).hits.hits[0]?._source
  }

  async getTagAliasesWithNames(names) {
    let aliases = []

    for (let i = 0; i < names.length / 1024; i++) {
      let theseNames = names.slice(i * 1024, i * 1024 + 1024)

      let dbAliases = (await this.database.search({
        index: "tagaliases",
        size: 1024,
        query: {
          bool: {
            should: theseNames.map(name => ({ term: { name } }))
          }
        }
      })).hits.hits.map(t => t._source)

      aliases = aliases.concat(dbAliases)
    }

    return aliases
  }

  async addTagAlias(tagAlias) {
    tagAlias.updatedAt = new Date()
    await this.database.index({
      index: "tagaliases",
      id: tagAlias.id.toString(),
      document: tagAlias
    })
  }

  async updateTagAlias(tagAlias) {
    tagAlias.updatedAt = new Date()
    await this.database.update({ index: "tagaliases", id: tagAlias.id.toString(), doc: tagAlias })
  }

  async deleteTagAlias(id) {
    await this.database.delete({ index: "tagaliases", id: id.toString() })
  }

  async getLatestTagImplicationId() {
    return (await this.database.search({
      index: "tagimplications",
      size: 1,
      sort: { id: "desc" },
      query: { match_all: {} }
    })).hits.hits[0]._source.id
  }

  async getTagImplication(id) {
    if (await this.database.exists({ index: "tagimplications", id: id.toString() })) {
      return (await this.database.get({ index: "tagimplications", id: id.toString() }))._source
    }

    return null
  }

  async getTagImplicationsWithIds(ids) {
    return (await this.database.mget({ index: "tagimplications", ids })).docs.filter(d => d.found).map(d => d._source)
  }

  async addTagImplication(tagImplication) {
    tagImplication.updatedAt = new Date()
    await this.database.index({
      index: "tagimplications",
      id: tagImplication.id.toString(),
      document: tagImplication
    })
  }

  async updateTagImplication(tagImplication) {
    tagImplication.updatedAt = new Date()
    await this.database.update({ index: "tagimplications", id: tagImplication.id.toString(), doc: tagImplication })
  }

  async deleteTagImplication(id) {
    await this.database.delete({ index: "tagimplications", id: id.toString() })
  }

  async convertPostTagIdsToTags(post) {
    let asFlat = post.tags.flat()
    let usedTags = await this.getTagsWithIds(asFlat)

    for (let tagGroup of post.tags) {
      for (let i = 0; i < tagGroup.length; i++) {
        try {
          tagGroup[i] = usedTags.find(t => t.id == tagGroup[i]).name
        } catch (e) {
          console.error("Error converting tags ids to tags")
          console.error(post)
          console.error(asFlat)
          console.error(usedTags)
          console.error(`Tag: ${tagGroup[i]}`)
        }
      }
    }
  }

  async convertPostsTagIdsToTags(posts) {
    for (let post of posts) {
      await this.convertPostTagIdsToTags(post)
    }
  }

  addFileUrlToPost(post) {
    if (post.isDeleted) return

    post.fileUrl = `https://static1.e621.net/data/${post.md5.slice(0, 2)}/${post.md5.slice(2, 4)}/${post.md5}.${post.fileType}`
    post.sampleUrl = `https://static1.e621.net/data/sample/${post.md5.slice(0, 2)}/${post.md5.slice(2, 4)}/${post.md5}.jpg`
    post.previewUrl = `https://static1.e621.net/data/preview/${post.md5.slice(0, 2)}/${post.md5.slice(2, 4)}/${post.md5}.jpg`
    post.croppedUrl = `https://static1.e621.net/data/crop/${post.md5.slice(0, 2)}/${post.md5.slice(2, 4)}/${post.md5}.jpg`
  }

  addFileUrlToPosts(posts) {
    for (let post of posts) {
      this.addFileUrlToPost(post)
    }
  }

  async processPostSearchResponse(posts) {
    await this.convertPostsTagIdsToTags(posts)
    this.addFileUrlToPosts(posts)
  }

  parseSize(value) {
    try {
      return filesizeParser(value)
    } catch {
      return false
    }
  }

  // This can be done better, but I'm tired
  parseRangeSyntax(value, field, type) {
    let query = {}

    if (type == "number") {
      if (!isNaN(value)) {
        query.term = {}
        query.term[field] = field != "mpixels" ? parseFloat(value) : parseFloat(value) * 1000000
        return query
      }
    } else if (type == "date") {
      let regex = /^\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d\.\d+([+-][0-2]\d:[0-5]\d|Z$)/

      if (regex.test(value)) {
        query.term = {}
        query.term[field] = new Date(value).toISOString()
        return query
      }
    } else if (type == "size") {
      let parsed = this.parseSize(value)

      if (parsed) {
        query.term = {}
        query.term[field] = parsed
        return query
      }
    }

    if (value.includes("..")) {
      let [left, right] = value.split("..")

      if (type == "number") {
        if (isNaN(left) || isNaN(right)) return false

        query.range = {}
        query.range[field] = {
          gte: field != "mpixels" ? parseFloat(left) : parseFloat(left) * 1000000,
          lte: field != "mpixels" ? parseFloat(right) : parseFloat(right) * 1000000
        }

        return query
      } else if (type == "date") {
        let regex = /^\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d\.\d+([+-][0-2]\d:[0-5]\d|Z$)/
        if (!regex.test(left) || !regex.test(right)) return false

        let dateLeft = new Date(left)
        let dateRight = new Date(right)

        query.range = {}
        query.range[field] = {
          gte: dateLeft.toISOString(),
          lte: dateRight.toISOString()
        }

        return query
      } else if (type == "size") {
        let parsedLeft = this.parseSize(left)
        let parsedRight = this.parseSize(right)

        if (!parsedLeft || !parsedRight) return false

        query.range = {}
        query.range[field] = {
          gte: parsedLeft,
          lte: parsedRight
        }

        return query
      }
    } else if (value.startsWith(">=")) {
      let right = value.slice(2)
      if (type == "number") {
        if (isNaN(right)) return false

        query.range = {}
        query.range[field] = {
          gte: field != "mpixels" ? parseFloat(right) : parseFloat(right) * 1000000
        }

        return query
      } else if (type == "date") {
        let regex = /^\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d\.\d+([+-][0-2]\d:[0-5]\d|Z$)/
        if (!regex.test(right)) return false

        let date = new Date(right)

        query.range = {}
        query.range[field] = {
          gte: date.toISOString()
        }

        return query
      } else if (type == "size") {
        let right = this.parseSize(right)

        if (!right) return false

        query.range = {}
        query.range[field] = {
          gte: right
        }

        return query
      }
    } else if (value.startsWith("<=")) {
      let right = value.slice(2)
      if (type == "number") {
        if (isNaN(right)) return false

        query.range = {}
        query.range[field] = {
          lte: field != "mpixels" ? parseFloat(right) : parseFloat(right) * 1000000
        }

        return query
      } else if (type == "date") {
        let regex = /^\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d\.\d+([+-][0-2]\d:[0-5]\d|Z$)/
        if (!regex.test(right)) return false

        let date = new Date(right)

        query.range = {}
        query.range[field] = {
          lte: date.toISOString()
        }

        return query
      } else if (type == "size") {
        let right = this.parseSize(right)

        if (!right) return false

        query.range = {}
        query.range[field] = {
          lte: right
        }

        return query
      }
    } else if (value.startsWith(">")) {
      let right = value.slice(1)
      if (type == "number") {
        if (isNaN(right)) return false

        query.range = {}
        query.range[field] = {
          gt: field != "mpixels" ? parseFloat(right) : parseFloat(right) * 1000000
        }

        return query
      } else if (type == "date") {
        let regex = /^\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d\.\d+([+-][0-2]\d:[0-5]\d|Z$)/
        if (!regex.test(right)) return false

        let date = new Date(right)

        query.range = {}
        query.range[field] = {
          gt: date.toISOString()
        }

        return query
      } else if (type == "size") {
        let right = this.parseSize(right)

        if (!right) return false

        query.range = {}
        query.range[field] = {
          gt: right
        }

        return query
      }
    } else if (value.startsWith("<")) {
      let right = value.slice(1)
      if (type == "number") {
        if (isNaN(right)) return false

        query.range = {}
        query.range[field] = {
          lt: field != "mpixels" ? parseFloat(right) : parseFloat(right) * 1000000
        }

        return query
      } else if (type == "date") {
        let regex = /^\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d\.\d+([+-][0-2]\d:[0-5]\d|Z$)/
        if (!regex.test(right)) return false

        let date = new Date(right)

        query.range = {}
        query.range[field] = {
          lt: date.toISOString()
        }

        return query
      } else if (type == "size") {
        let right = this.parseSize(right)

        if (!right) return false

        query.range = {}
        query.range[field] = {
          lt: right
        }

        return query
      }
    }
  }

  metaTagParser(tag) {
    let match = META_MATCH_REGEX.exec(tag)

    if (match) {
      let [fullMatch, tagName, value] = match.filter(m => m)

      tagName = META_TAGS_TO_FIELD_NAMES[tagName]

      switch (tagName) {
        case "randomSeed": {
          let seed = parseInt(value)

          if (isNaN(seed)) return { ignore: true }

          return { isOrderTag: true, randomSeed: seed }
        }

        case "order":
          {
            if (value == "random") {
              return { isOrderTag: true, random: true }
            }

            let split = value.split("_")
            if (split.length > 2) split = [split.slice(0, -1).join("_"), split[split.length - 1]]
            let sortOrder = split.length == 2 ? split[1] : "desc"

            if (split[0] == "rank") {
              return { isOrderTag: true, rank: true, sortOrder }
            }

            if (!SORTABLE_FIELDS[split[0]]) {
              return { ignore: true }
            }

            if ((sortOrder != "desc" && sortOrder != "asc")) return { ignore: true }

            if (typeof (SORTABLE_FIELDS[split[0]]) != "string") {
              return { isOrderTag: true, asQuery: SORTABLE_FIELDS[split[0]] }
            }

            let asQuery = {}

            asQuery[SORTABLE_FIELDS[split[0]]] = sortOrder

            return { isOrderTag: true, asQuery: [asQuery] }
          }


        case "uploaderId":
        case "approverId":
          {
            if (value.startsWith("!")) value = value.slice(1)
            if (value == "" || isNaN(value)) return { ignore: true }

            let asQuery = {}

            asQuery[tagName] = parseInt(value)

            return { isOrderTag: false, asQuery: { term: asQuery } }
          }

        case "id":
        case "score":
        case "favoriteCount":
        case "commentCount":
        case "width":
        case "height":
        case "mpixels":
        case "fileSize":
        case "createdAt":
        case "duration":
        case "parentId":
        case "ratio":
          {
            if (value == "") return { ignore: true }

            let asQuery = this.parseRangeSyntax(value, tagName, "number")

            if (!asQuery) return { ignore: true }

            return { isOrderTag: false, asQuery: asQuery }
          }

        case "general":
        case "artist":
        case "character":
        case "copyright":
        case "species":
        case "invalid":
        case "lore":
        case "meta":
          {
            let op = "=="
            if (value == "") return { ignore: true }

            if (isNaN(value)) {
              if (value.startsWith(">") || value.startsWith("<")) {
                op = value.slice(0, 1)
                value = value.slice(1)
              } else if (value.startsWith(">=") || value.startsWith("<=")) {
                op = value.slice(0, 2)
                value = value.slice(2)
              } else {
                return { ignore: true }
              }

              if (isNaN(value)) return { ignore: true }
            }

            return {
              isOrderTag: false,
              asQuery: {
                script: {
                  script: {
                    lang: "painless",
                    source: `doc.tags[params.category] ${op} params.value`,
                    params: {
                      value: parseInt(value),
                      category: TAG_CATEGORIES_TO_CATEGORY_ID[tagName]
                    }
                  }
                }
              }
            }
          }

        case "tagCount":
          {
            if (value == "") return { ignore: true }

            let op = "=="
            if (value == "") return { ignore: true }

            if (isNaN(value)) {
              if (value.startsWith(">") || value.startsWith("<")) {
                op = value.slice(0, 1)
                value = value.slice(1)
              } else if (value.startsWith(">=") || value.startsWith("<=")) {
                op = value.slice(0, 2)
                value = value.slice(2)
              } else {
                return { ignore: true }
              }

              if (isNaN(value)) return { ignore: true }
            }

            return {
              isOrderTag: false,
              asQuery: {
                script: {
                  script: {
                    lang: "painless",
                    source: `doc.flattenedTags.size() ${op} params.value`,
                    params: {
                      value: parseInt(value)
                    }
                  }
                }
              }
            }
          }

        case "hasSource":
          {
            if (value != "true" && value != "false") return { ignore: true }

            if (value == "true") {
              return {
                isOrderTag: false,
                asQuery: {
                  script: {
                    script: {
                      lang: "painless",
                      source: `doc.sources.size() > params.value`,
                      params: {
                        value: 0
                      }
                    }
                  }
                }
              }
            } else {
              return {
                isOrderTag: false,
                asQuery: {
                  script: {
                    script: {
                      lang: "painless",
                      source: `doc.sources.size() == params.value`,
                      params: {
                        value: 0
                      }
                    }
                  }
                }
              }
            }
          }

        case "rating":
          {
            if (value.length > 1) value = value.slice(0, 1)

            if (value != "s" && value != "q" && value != "e") return { ignore: true }

            return { isOrderTag: false, asQuery: { term: { rating: value } } }
          }

        case "fileType":
          {
            if (value != "png" && value != "jpg" && value != "gif" && value != "swf" && value != "webm") return { ignore: true }

            return { isOrderTag: false, asQuery: { term: { fileType: value } } }
          }

        case "isChild":
          {
            if (value != "true" && value != "false") return { ignore: true }

            if (value == "true") {
              return { isOrderTag: false, asQuery: { exists: { field: "parentId" } } }
            } else {
              return { isOrderTag: false, asQuery: { bool: { must_not: { exists: { field: "parentId" } } } } }
            }
          }

        case "isRatingLocked":
        case "isNoteLocked":
          {
            if (value != "true" && value != "false") return { ignore: true }

            let asQuery = { term: {} }

            if (value == "true") {
              asQuery.term[tagName] = true
              return { isOrderTag: false, asQuery }
            } else {
              asQuery.term[tagName] = false
              return { isOrderTag: false, asQuery }
            }
          }

        case "sources":
          {
            let asQuery = {}
            if (!value.includes("*")) {
              asQuery.term = {}
              asQuery.term[`${tagName}.keyword`] = value
            } else {
              asQuery.wildcard = {}
              asQuery.wildcard[`${tagName}.keyword`] = {
                value,
                case_insensitive: true,
                rewrite: "constant_score"
              }
            }
            return { isOrderTag: false, asQuery }
          }
        case "md5":
          {
            let asQuery = { term: {} }
            asQuery.term[tagName] = value
            return { isOrderTag: false, asQuery }
          }

        case "status":
          {
            if (value == "pending") {
              return { isOrderTag: false, asQuery: { term: { isPending: true } } }
            } else if (value == "active") {
              return { isOrderTag: false, asQuery: { term: { isPending: false } } }
            } else if (value == "deleted") {
              return { isOrderTag: false, statusDeleted: true, asQuery: { term: { isDeleted: true } } }
            } else if (value == "flagged") {
              return { isOrderTag: false, asQuery: { term: { isFlagged: true } } }
            } else if (value == "modqueue") {
              return {
                isOrderTag: false,
                asQuery: {
                  bool: {
                    should: [
                      { term: { isFlagged: true } },
                      { term: { isPending: true } }
                    ]
                  }
                }
              }
            } else if (value == "any") {
              return { statusDeleted: true }
            }
          }

        case "isParent":
          {
            return {
              isOrderTag: false,
              asQuery: {
                script: {
                  script: {
                    lang: "painless",
                    source: `doc.children.size() > params.value`,
                    params: {
                      value: 0
                    }
                  }
                }
              }
            }
          }

        // This may not get done, this would be incredibly complicated and I'm not sure yet how I would go about doing it.
        // case "inParent":
        // case "inChild":
        // case "inAncestor":
        // case "inDescendant":
        //   {
        //     return {
        //       [`next${tagName.charAt(0).toUpperCase()}${tagName.slice(1)}`]: true
        //     }
        //   }

        default:
          return { ignore: true }
      }
    }
  }

  getGroups(tags) {
    if (tags.length == 0) return [true, null]
    let tokenizer = new Tokenizer(tags)
    let currentGroupIndex = []
    let group = { tokens: [], groups: [], orderTags: [], parsedMetaTags: [] }

    for (let token of tokenizer) {
      let curGroup = group
      for (let group of currentGroupIndex) {
        curGroup = curGroup.groups[group]
      }

      if (token == "(") {
        currentGroupIndex.push(curGroup.groups.length)
        curGroup.groups.push({ tokens: [], groups: [], parsedMetaTags: [] })
        curGroup.tokens.push(`__${curGroup.groups.length - 1}`)
      } else if (token == ")") {
        currentGroupIndex.splice(currentGroupIndex.length - 1, 1)
      } else {
        let parsedMetaTag = this.metaTagParser(token)
        if (parsedMetaTag) {
          if (parsedMetaTag.isOrderTag) {
            if (parsedMetaTag.random) {
              group.orderTags.push({ random: true })
            } else if (parsedMetaTag.randomSeed !== undefined) {
              group.orderTags.push({ randomSeed: parsedMetaTag.randomSeed })
            } else if (parsedMetaTag.rank) {
              group.orderTags.push(parsedMetaTag)
            } else {
              group.orderTags = group.orderTags.concat(parsedMetaTag.asQuery)
            }
          } else if (!parsedMetaTag.ignore) {
            curGroup.parsedMetaTags.push(parsedMetaTag)
            curGroup.tokens.push(`--${curGroup.parsedMetaTags.length - 1}`)
          }
        } else {
          curGroup.tokens.push(token.toLowerCase())
        }
      }
    }

    if (currentGroupIndex.length != 0) {
      return [false, { status: 400, message: "Malformed tags, group not closed" }]
    }

    return [true, group]
  }

  async convertToTagIds(group) {
    for (let i = 0; i < group.tokens.length; i++) {
      let token = group.tokens[i]
      if (!TOKENS_TO_SKIP.includes(token) && !token.startsWith("__") && !token.startsWith("--")) {
        if (token.includes("*") || token.includes("?")) {

          let tags = []

          let res = await this.database.search({
            size: 1024, index: "tags", sort: { id: "desc" }, scroll: "1m", query: {
              regexp: {
                name: {
                  value: token.replace(/[/\-\\^$+().|[\]{}]/g, "\\$&").replace("*", ".*").replace("?", "."),
                  case_insensitive: true,
                  flags: "ALL",
                  rewrite: "constant_score"
                }
              }
            }
          })

          tags = tags.concat(res.hits.hits.map(t => t._source))

          while (res.hits.hits.length > 0) {
            res = await this.database.scroll({ scroll_id: res._scroll_id, scroll: "1m" })
            tags = tags.concat(res.hits.hits.map(t => t._source))
          }

          for (let j = 0; j < tags.length; j++) {
            group.tokens[i++] = tags[j].id
            if (j < tags.length - 1) group.tokens[i++] = "~"
          }
        } else {
          let tagId = await this.getAliasOrTagId(token)

          if (tagId) {
            group.tokens[i] = tagId
          } else {
            group.tokens[i] = ""
          }
        }
      }
    }

    group.tokens = group.tokens.filter(t => t != "")

    for (let g of group.groups) {
      await this.convertToTagIds(g)
    }
  }

  // ( a b c) ~ ( d e f ) means > (a & b & c) OR (d & e & f)
  //  a b ( c d ) means > a & b & c & d (nothing special)
  // -a b c means > not a, b & c
  // a b -( c d ) means > a & b, not (c & d) (meaning it can have either c or d, but not both)
  // a ~ b c means > (a OR b) & c
  // a ~ b ~ c d means > (a OR b OR c) & d
  // a ~ b ~ ( d e ) means > (a OR b OR (d and e)) (either a or b or the post has both d and e)
  // a ~ b ( -c ~ -e ) means > (a OR b & (not c OR not e)) (meaning a or b AND the post doesn't have c or the post doesn't have e)
  // ( -a ~ b ) means > (not a) OR b
  // a ( b ( c ) ) means > a & b & c
  // a ( b ~ ( c e ) ) means > a & (b OR (c and e))

  async buildQueryFromGroup(group, hasDeleted = false, curQuery = { must: [], should: [], must_not: [] }) {
    let modifier = MODIFIERS.NONE

    let mentionsDeleted = hasDeleted || group.parsedMetaTags.some(t => t.statusDeleted)

    for (let i = 0; i < group.tokens.length; i++) {
      let token = group.tokens[i]
      if (TOKENS_TO_SKIP.includes(token) || token == "") continue

      let previousToken = i > 0 ? group.tokens[i - 1] : null
      let previousNegate = previousToken == "-"
      let nextToken = i < group.tokens.length - 1 ? group.tokens[i + 1] : null

      if (nextToken == "~") modifier = MODIFIERS.OR

      if (typeof (token) == "number" || (!token.startsWith("__") && !token.startsWith("--"))) {
        if (modifier == MODIFIERS.NONE) {
          if (!previousNegate) {
            curQuery.must.push({
              term: {
                flattenedTags: token
              }
            })
          } else {
            curQuery.must_not.push({
              term: {
                flattenedTags: token
              }
            })
          }
        } else if (modifier == MODIFIERS.OR) {
          if (!previousNegate) {
            curQuery.should.push({
              term: {
                flattenedTags: token
              }
            })
          } else {
            curQuery.should.push({
              bool: {
                must_not: {
                  term: {
                    flattenedTags: token
                  }
                }
              }
            })
          }
        }
      } else {
        if (token.startsWith("__")) {
          let nextGroup = group.groups[parseInt(token.slice(2))]

          let query = { must: [], should: [], must_not: [] }

          this.buildQueryFromGroup(nextGroup, mentionsDeleted, query)

          if (modifier == MODIFIERS.NONE) {
            if (!previousNegate) curQuery.must.push({ bool: query })
            else curQuery.must_not.push({ bool: query })
          } else if (modifier == MODIFIERS.OR) {
            if (!previousNegate) curQuery.should.push({ bool: query })
            else {
              curQuery.should.push({
                bool: {
                  must_not: { bool: query }
                }
              })
            }
          }
        } else if (token.startsWith("--")) {
          let parsedMetaTag = group.parsedMetaTags[parseInt(token.slice(2))]

          if (parsedMetaTag.asQuery == null) continue

          if (modifier == MODIFIERS.NONE) {
            if (!previousNegate) curQuery.must.push(parsedMetaTag.asQuery)
            else curQuery.must_not.push(parsedMetaTag.asQuery)
          } else if (modifier == MODIFIERS.OR) {
            if (!previousNegate) curQuery.should.push(parsedMetaTag.asQuery)
            else {
              curQuery.should.push({
                bool: {
                  must_not: parsedMetaTag.asQuery
                }
              })
            }
          }
        }
      }

      if (modifier == MODIFIERS.OR && nextToken != "~") modifier = MODIFIERS.NONE
    }

    if (!mentionsDeleted) {
      curQuery.must_not.push({ term: { isDeleted: true } })
    }

    if (curQuery.should.length > 0) curQuery.minimum_should_match = 1

    return curQuery
  }

  async performSearch(query, limit = 50, searchAfter = null) {
    try {
      let success, group

      let req = {
        size: limit, index: "posts", sort: { id: "desc" }, _source_excludes: ["flattenedTags"]
      }

      if (query) {
        [success, group] = this.getGroups(query.trim())

        if (!success) {
          console.log(group)
          return group
        }

        if (group != null) {
          await this.convertToTagIds(group)

          let databaseQuery = await this.buildQueryFromGroup(group)

          req.query = { bool: databaseQuery }

          if (group.orderTags.length > 0) {
            let randomIndex = group.orderTags.findIndex(tag => tag.random)
            let randomSeedIndex = group.orderTags.findIndex(tag => tag.randomSeed !== undefined)

            if (randomIndex == -1) {
              if (randomSeedIndex != -1) {
                group.orderTags.splice(randomSeedIndex, 1)
              }

              let rankIndex = group.orderTags.findIndex(tag => tag.rank)

              let sortOrder

              if (rankIndex != -1) {
                sortOrder = group.orderTags[rankIndex].sortOrder
                group.orderTags.splice(rankIndex, 1)

                req.query.bool.must.push({ range: { score: { gt: 0 } } })
                req.query.bool.must.push({ range: { createdAt: { gte: new Date(Date.now() - 172800000) } } })

                req.query = {
                  script_score: {
                    query: req.query,
                    script: {
                      lang: "painless",
                      source: "Math.log(doc['score'].value) / params.log3 + (doc['createdAt'].value.getMillis() / 1000 - params.e6StartDate) / 35000",
                      params: { log3: Math.log(3), e6StartDate: 1116936000 },
                    }
                  }
                }
              }

              req.sort = group.orderTags

              if (rankIndex != -1) {
                req.sort.unshift({ _score: sortOrder })
              }

            } else {
              let randomScore = {}

              if (randomSeedIndex != -1) {
                randomScore.seed = group.orderTags[randomSeedIndex].randomSeed
              } else {
                randomScore.seed = Date.now()
              }

              req.query = {
                function_score: {
                  query: req.query,
                  random_score: randomScore
                },
              }
            }
          }
        }
      }

      if (searchAfter) {
        if (typeof (searchAfter) == "number") {
          if (searchAfter <= 0) {
            return { status: 400, message: "Invalid page. Must be greater than 0" }
          }
          req.from = (searchAfter - 1) * limit

          if (req.from >= 10000) {
            return { status: 400, message: "Invalid page. (page - 1) * limit >= 10000" }
          }

        } else {
          req.search_after = searchAfter
        }
      }

      let res = await this.database.search(req)

      let posts = res.hits.hits.map(hit => hit._source)

      await this.processPostSearchResponse(posts)

      let response = { posts }

      if (res.hits.hits.length > 0) response.searchAfter = res.hits.hits[res.hits.hits.length - 1].sort

      return response
    } catch (e) {
      console.error(e)
      return { status: 500, message: "Internal server error" }
    }
  }

  async getDirectTagRelationships(tagName, includes) {
    let [tag, isAlias] = await this.getOrAddAliasOrTag(tagName)

    if (!tag) return {}

    let query = {
      bool: {
        should: [],
        minimum_should_match: 1
      }
    }

    if (includes.includes("parents")) {
      query.bool.should.push({
        term: {
          antecedentId: tag.id
        }
      })
    }

    if (includes.includes("children")) {
      query.bool.should.push({
        term: {
          consequentId: tag.id
        }
      })
    }

    let res = await this.database.search({
      size: 10000, index: "tagimplications", sort: { id: "desc" }, query
    })

    let relationships = res.hits.hits.map(hit => hit._source)

    let antecedes = []
    let consequents = []

    // TODO: Batch the shit out of this, getting every tag and tag alias one at a time is way too slow.

    let idsToGet = relationships.map(r => r.antecedentId == tag.id ? r.consequentId : r.antecedentId)

    if (idsToGet.length == 0) return {
      thisTag: tag,
      parents: [],
      children: []
    }

    let antecedeOrConsequent = relationships.map(r => r.antecedentId == tag.id ? ({ antecede: true, tagId: r.consequentId }) : ({ antecede: false, tagId: r.antecedentId }))
    let tags = await this.getTagsWithIds(idsToGet)

    // This has never once gotten an existing tag.

    // let newTagIds = idsToGet.filter(id => !tags.find(t => t.id == id))

    // for (let id of newTagIds) {
    //   if (id == tag.id) continue

    //   let t = await this.getNewTagById(id)

    //   if (t) {
    //     let isAntecede = antecedeOrConsequent.find(c => c.tagId == id).antecede

    //     if (isAntecede) antecedes.push(t)
    //     else consequents.push(t)
    //   }
    // }

    let aliases = await this.getTagAliasesWithNames(tags.map(t => t.name))

    let withoutAliases = tags.filter(t => !aliases.find(a => a.name == t.name))

    for (let t of withoutAliases) {
      if (t.id == tag.id) continue

      let isAntecede = antecedeOrConsequent.find(c => c.tagId == t.id).antecede

      if (isAntecede) antecedes.push(t)
      else consequents.push(t)

    }

    // for (let relationship of relationships) {
    //   if (relationship.antecedentId == tag.id) {
    //     let t = await this.getTag(relationship.consequentId)

    //     if (t) { // See: face_tuft. Angry
    //       let alias = await this.getTagAliasByName(t.name)
    //       if (alias) {
    //         t = null //await this.getTag(alias.consequentId) ALAISES DO NOT TRANSFER THEIR IMPLICATIONS FOR SOME GODFORSAKEN REASON
    //       }
    //     } else {
    //       t = await this.getNewTagById(relationship.consequentId)
    //     }

    //     if (t && t.id != tag.id) antecedes.push(t)
    //   } else {
    //     let t = await this.getTag(relationship.antecedentId)

    //     if (t) { // See: face_tuft. Angry
    //       let alias = await this.getTagAliasByName(t.name)
    //       if (alias) {
    //         t = null //await this.getTag(alias.consequentId) ALAISES DO NOT TRANSFER THEIR IMPLICATIONS FOR SOME GODFORSAKEN REASON
    //       }
    //     } else {
    //       t = await this.getNewTagById(relationship.antecedentId)
    //     }

    //     if (t && t.id != tag.id) consequents.push(t)
    //   }
    // }

    return {
      thisTag: tag,
      parents: antecedes,
      children: consequents
    }
  }

  async getAllParentRelationships(tagName) {
    let [tag, isAlias] = await this.getOrAddAliasOrTag(tagName)

    if (!tag) return {}

    let query = {
      bool: {
        should: [{
          term: {
            antecedentId: tag.id
          }
        }],
        minimum_should_match: 1
      }
    }

    let res = await this.database.search({
      size: 10000, index: "tagimplications", sort: { id: "desc" }, query
    })

    let relationships = res.hits.hits.map(hit => hit._source)

    let antecedes = []

    for (let relationship of relationships) {
      if (relationship.antecedentId == tag.id) {
        let t = await this.getTag(relationship.consequentId)

        if (t.id == tag.id) continue

        antecedes.push(t)
      }
    }

    return {
      thisTag: tag,
      parents: antecedes.length == 0 ? [] : await Promise.all(antecedes.map(async p => ({ thisTag: p, parents: (await this.getAllParentRelationships(p.name)).parents })))
    }
  }
}

module.exports = Utilities