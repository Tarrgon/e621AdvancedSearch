// ==UserScript==
// @name         e621 Advanced Search
// @namespace    e621advanced.search
// @version      0.1
// @description  A much more powerful search syntax for e621
// @author       DefinitelyNotAFurry4
// @match        https://e621.net/*
// @icon         https://www.google.com/s2/favicons?sz=64&domain=e621.net
// @updateURL    https://raw.githubusercontent.com/DontTalkToMeThx/e621AdvancedSearch/releases/latest/e621AdvancedSearch.meta.js
// @downloadURL  https://raw.githubusercontent.com/DontTalkToMeThx/e621AdvancedSearch/releases/latest/e621AdvancedSearch.user.js
// @grant        GM_addElement
// @grant        GM.xmlHttpRequest
// @grant        unsafeWindow
// @connect      search.yiff.today
// @run-at       document-end
// ==/UserScript==

let searchData = {
  searchAfter: null,
  searchText: "",
  loading: false
}

let favorites = []

async function getFavorites() {
  return new Promise(resolve => {
    GM.xmlHttpRequest({
      method: "GET",
      url: "https://e621.net/favorites.json",
      onload: function (response) {
        favorites = JSON.parse(response.responseText).posts.map(p => p.id)
        resolve()
      }
    })
  })
}

async function getPostIdsInPool(id) {
  return new Promise(resolve => {
    GM.xmlHttpRequest({
      method: "GET",
      url: `https://e621.net/pools.json?search[id]=${id}`,
      onload: function (response) {
        let data = JSON.parse(response.responseText)

        if (data.length == 0) return resolve([])

        resolve(data[0].post_ids)
      }
    })
  })
}

async function getPostIdsInSet(id) {
  return new Promise(resolve => {
    GM.xmlHttpRequest({
      method: "GET",
      url: `https://e621.net/post_sets.json?search[id]=${id}`,
      onload: function (response) {
        let data = JSON.parse(response.responseText)

        if (data.post_sets) return resolve([])

        resolve(data[0].post_ids)
      }
    })
  })
}

function clearPostsContainer() {
  let postsContainer = document.getElementById("posts-container")

  while (postsContainer.hasChildNodes()) {
    postsContainer.removeChild(postsContainer.firstChild)
  }
}

function deletePaginator() {
  let posts = document.getElementById("posts")
  let paginator = posts.getElementsByClassName("paginator")[0]

  if (paginator) paginator.remove()
}

async function handleInfiniteScroll() {
  if (!searchData.loading) {
    const endOfPage = window.innerHeight + window.scrollY + 250 >= document.body.offsetHeight

    if (endOfPage) {
      await executeSearch(searchData.searchText)
    }

    if (searchData.searchAfter == null) {
      window.removeEventListener("scroll", handleInfiniteScroll)
    }
  }
}

function createPost(post, searchText) {
  let article = document.createElement("article")
  article.id = `post_${post.id}`
  article.classList.add("post-preview")

  let tags = post.tags.flat()

  if (post.rating == "e") article.classList.add("post-rating-explicit")
  else if (post.rating == "q") article.classList.add("post-rating-questionable")
  else if (post.rating == "s") article.classList.add("post-rating-safe")

  let status = "active"

  if (post.isDeleted) {
    status = "deleted"
    article.classList.add("post-status-deleted")
  } else if (post.isFlagged) {
    status = "flagged"
    article.classList.add("post-status-flagged")
  } else if (post.isPending) {
    status = "pending"
    article.classList.add("post-status-pending")
  }

  let statusFlags = []

  if (post.parentId) {
    statusFlags.push("P")
    article.classList.add("post-status-has-parent")
  }

  if (post.children.length > 0) {
    statusFlags.push("C")
    article.classList.add("post-status-has-children")
  }

  if (post.isPending) statusFlags.push("U")

  if (post.isFlagged) statusFlags.push("F")

  article.setAttribute("data-tags", tags.join(" "))
  article.setAttribute("data-rating", post.rating)
  article.setAttribute("data-flags", post.isFlagged ? "flagged" : (post.isPending ? "pending" : ""))
  article.setAttribute("data-uploader-id", post.uploaderId)
  article.setAttribute("data-uploader", "")
  article.setAttribute("data-file-ext", post.fileType)
  article.setAttribute("data-score", `${post.score}`)
  article.setAttribute("data-fav-count", `${post.favoriteCount}`)
  article.setAttribute("data-is-favorited", favorites.includes(post.id))
  article.setAttribute("data-file-url", post.fileUrl)
  article.setAttribute("data-large-file-url", post.sampleUrl)
  article.setAttribute("data-preview-file-url", post.previewUrl)

  let a = document.createElement("a")
  if (searchData.searchAfter != null) a.href = `/posts/${post.id}?q=${searchText}&search_after=${encodeURIComponent(JSON.stringify(searchData.searchAfter))}`
  else a.href = `/posts/${post.id}?q=${searchText}`
  article.appendChild(a)

  let picture = document.createElement("picture")
  a.appendChild(picture)

  if (!post.isDeleted) {
    let source1 = document.createElement("source")
    source1.media = "(max-width: 800px)"
    source1.srcset = post.croppedUrl
    picture.append(source1)

    let source2 = document.createElement("source")
    source2.media = "(min-width: 800px)"
    source2.srcset = post.previewUrl
    picture.append(source2)
  } else {
    let source = document.createElement("source")
    source.media = "(min-width: 0px)"
    source.srcset = "/images/deleted-preview.png"
    picture.append(source)
  }

  let img = document.createElement("img")
  img.classList.add("has-cropped-true")
  img.title =
    `Rating: ${post.rating}
ID: ${post.id}
Date: ${new Date(post.createdAt).toString()}
Status: ${status}
Score: ${post.score}

${tags.join(" ")}`
  img.alt = tags.join(" ")
  picture.append(img)

  let desc = document.createElement("div")
  desc.classList.add("desc")
  article.appendChild(desc)

  let score = document.createElement("div")
  score.id = `post-score-${post.id}`
  score.classList.add("post-score")
  desc.appendChild(score)

  let postScore = document.createElement("span")
  postScore.classList.add("post-score-score")
  if (post.score == 0) {
    postScore.classList.add("score-neutral")
    postScore.innerText = "↕0"
  } else if (post.score > 0) {
    postScore.classList.add("score-positive")
    postScore.innerText = `↑${post.score}`
  } else if (post.score < 0) {
    postScore.classList.add("score-negative")
    postScore.innerText = `↓${post.score}`
  }
  score.appendChild(postScore)

  let favoritesScore = document.createElement("span")
  favoritesScore.classList.add("post-score-faves")
  favoritesScore.innerText = `♥${post.favoriteCount}`
  score.appendChild(favoritesScore)

  let comments = document.createElement("span")
  comments.classList.add("post-score-comments")
  comments.innerText = `C${post.commentCount}`
  score.appendChild(comments)

  let rating = document.createElement("span")
  rating.classList.add("post-score-rating")
  rating.innerText = post.rating.toUpperCase()
  score.appendChild(rating)

  let extras = document.createElement("span")
  extras.classList.add("post-score-extras")
  extras.innerText = statusFlags.join("")
  score.appendChild(extras)

  return article
}

// Thank you https://stackoverflow.com/questions/33631041/javascript-async-await-in-replace
async function replaceAsync(str, regex, asyncFn) {
  const promises = []
  str.replace(regex, (match, ...args) => {
    const promise = asyncFn(match, ...args)
    promises.push(promise)
  })
  const data = await Promise.all(promises)
  return str.replace(regex, () => data.shift())
}

async function executeSearch(searchText, page = null) {
  searchData.loading = true

  await getFavorites()

  searchText = await replaceAsync(searchText, new RegExp(/set:(\d+)/g), async (match, id) => {
    let ids = await getPostIdsInSet(id)
    
    if (ids.length > 0) return `( id:${ids.join(" ~ id:")} )`
    else return ""
  })

  searchText = await replaceAsync(searchText, new RegExp(/pool:(\d+)/g), async (match, id) => {
    let ids = await getPostIdsInPool(id)

    if (ids.length > 0) return `( id:${ids.join(" ~ id:")} )`
    else return ""
  })

  if (searchData.searchText != searchText) {
    searchData.searchAfter = null
  }

  searchData.searchText = searchText

  return new Promise((resolve) => {
    let postsContainer = document.getElementById("posts-container")

    let req = {
      method: "POST",
      url: `https://search.yiff.today/?query=${searchText}${page != null && searchData.searchAfter == null ? `&page=${page}` : ""}&limit=70`,
      onload: function (response) {
        let data = JSON.parse(response.responseText)

        for (let post of data.posts) {
          postsContainer.appendChild(createPost(post, searchText))
        }

        Danbooru.Blacklist.apply()

        if (data.posts.length > 0) {
          searchData.searchAfter = data.searchAfter
          window.addEventListener("scroll", handleInfiniteScroll)
        } else {
          searchData.searchAfter = null
        }

        unsafeWindow.history.pushState({}, "", `https://e621.net/posts?q=${searchText}${page != null && searchData.searchAfter == null ? `&page=${page}` : ""}`)

        resolve()

        searchData.loading = false
      }
    }

    if (searchData.searchAfter != null) {
      req.data = JSON.stringify({ searchAfter: searchData.searchAfter })
      req.headers = {
        "Content-Type": "application/json"
      }
    }

    GM.xmlHttpRequest(req)
  })
}

function createMainSearchForm() {
  let form = document.createElement("form")

  form.addEventListener("submit", (e) => {
    e.preventDefault()

    let searchText = e.target.querySelector("#tags").value

    if (e.submitter.id == "submit") unsafeWindow.location.href = `https://e621.net/posts?q=${searchText}`
    else unsafeWindow.location.href = `https://e621.net/posts?tags=${searchText}`
  })

  let div = document.createElement("div")
  form.appendChild(div)

  let input = document.createElement("input")
  input.id = "tags"
  input.type = "text"
  input.size = 30
  input.setAttribute("autofocus", "autofocus")
  input.setAttribute("data-autocomplete", "tag-query")
  input.setAttribute("autocomplete", "off")
  input.classList.add("ui-autocomplete-input")

  div.appendChild(input)
  div.appendChild(document.createElement("br"))

  let submitButton = document.createElement("input")
  submitButton.id = "submit"
  submitButton.type = "submit"
  submitButton.value = "Search"
  div.appendChild(submitButton)

  div.append(" ")

  let submitNormalButton = document.createElement("input")
  submitNormalButton.id = "submit-normal"
  submitNormalButton.type = "submit"
  submitNormalButton.value = "Search (normal)"
  div.appendChild(submitNormalButton)

  div.append(" ")

  let changeMascotButton = document.createElement("input")
  changeMascotButton.id = "change-mascot"
  changeMascotButton.type = "button"
  changeMascotButton.value = "Change Mascot"
  div.appendChild(changeMascotButton)

  return form
}

function createSearchBox() {
  let searchBox = document.createElement("section")
  searchBox.id = "search-box"

  let title = document.createElement("h1")
  title.innerHTML = "Search <span class='search-help'><a href='/help/cheatsheet'>(search help)</a></span>"
  searchBox.appendChild(title)

  let form = document.createElement("form")
  searchBox.appendChild(form)

  form.addEventListener("submit", (e) => {
    e.preventDefault()

    if (e.submitter.id == "submit-query") {
      if (window.location.pathname != "/posts") {
        unsafeWindow.location.href = `https://e621.net/posts?q=${e.target.querySelector("input").value}`
      } else {
        clearPostsContainer()
        deletePaginator()
        executeSearch(e.target.querySelector("input").value)
      }
    } else {
      unsafeWindow.location.href = `https://e621.net/posts?tags=${e.target.querySelector("input").value}`
    }
  })

  let input = document.createElement("input")
  input.type = "text"
  input.setAttribute("data-shortcut", "q")
  input.setAttribute("data-autocomplete", "tag-query")
  input.title = "Shortcut is q"
  input.classList.add("ui-autocomplete-input")
  input.setAttribute("autocomplete", "off")
  form.appendChild(input)

  let button = document.createElement("button")
  button.id = "submit-query"
  button.type = "submit"
  button.innerHTML = '<i class="fa-solid fa-magnifying-glass"></i>'
  form.appendChild(button)

  form.appendChild(document.createElement("br"))

  let buttonNormal = document.createElement("input")
  buttonNormal.id = "submit-query-normal"
  buttonNormal.type = "submit"
  buttonNormal.value = "Search normally"
  form.appendChild(buttonNormal)

  return searchBox
}

function replaceSearchBox() {
  if (window.location.pathname == "/") {
    let searchForm = document.getElementById("tags").parentElement.parentElement
    let parent = searchForm.parentElement
    searchForm.remove()
    parent.appendChild(createMainSearchForm())
  } else if (window.location.pathname.startsWith("/posts")) {
    let searchBox = document.getElementById("search-box")
    searchBox.remove()
    let sidebar = document.getElementById("sidebar")
    sidebar.insertBefore(createSearchBox(), sidebar.firstChild)
  }
}

function parseQuery(queryString) {
  let query = {}
  let pairs = (queryString[0] === "?" ? queryString.substr(1) : queryString).split("&")

  for (let i = 0; i < pairs.length; i++) {
    let pair = pairs[i].split("=")
    query[decodeURIComponent(pair[0])] = decodeURIComponent(pair.slice(1).join("=") || "")
  }

  return query
}

(function () {
  'use strict'

  replaceSearchBox()

  if (window.location.search.includes("q=")) {
    if (window.location.pathname == "/posts") {
      let query = parseQuery(window.location.search)

      if (query.search_after) {
        searchData.searchAfter = JSON.parse(query.search_after)
      }

      deletePaginator()
      executeSearch(query.q, query.page)
    } else if (window.location.pathname.startsWith("/posts/")) {
      let query = parseQuery(window.location.search)

      let searchSection = document.getElementById("search-box")
      let searchInput = searchSection.querySelector("input")

      searchInput.value = query.q
    }
  }
})()