"use strict";
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
Object.defineProperty(exports, "__esModule", { value: true });
var skip_runtime_1 = require("skip-runtime");
var HackerNewsService = /** @class */ (function () {
    function HackerNewsService() {
        this.inputTables = ["posts", "users", "upvotes"];
        this.resources = [PostsResource];
    }
    HackerNewsService.prototype.reactiveCompute = function (store, inputCollections) {
        var upvotes = inputCollections.upvotes.map(UpvotesMapper);
        var postsWithUpvotes = inputCollections.posts.map(PostsMapper, inputCollections.users, upvotes);
        return {
            postsWithUpvotes: postsWithUpvotes
        };
    };
    return HackerNewsService;
}());
var UpvotesMapper = /** @class */ (function () {
    function UpvotesMapper() {
    }
    UpvotesMapper.prototype.mapElement = function (key, it) {
        var value = it.first().post_id;
        return [[value, key]];
    };
    return UpvotesMapper;
}());
var PostsMapper = /** @class */ (function () {
    function PostsMapper(users, upvotes) {
        this.users = users;
        this.upvotes = upvotes;
    }
    PostsMapper.prototype.mapElement = function (key, it) {
        var post = it.first();
        var upvotes = this.upvotes.getArray(key).length;
        var author = this.users.getOne(post.author_id);
        return [[[-upvotes, key], __assign(__assign({}, post), { upvotes: upvotes, author: author })]];
    };
    return PostsMapper;
}());
var PostsCleanupKeyMapper = /** @class */ (function () {
    function PostsCleanupKeyMapper() {
    }
    PostsCleanupKeyMapper.prototype.mapElement = function (key, it) {
        var post = it.first();
        return [[key[1], post]];
    };
    return PostsCleanupKeyMapper;
}());
var PostsResource = /** @class */ (function () {
    function PostsResource(limit) {
        this.limit = limit;
    }
    PostsResource.prototype.reactiveCompute = function (store, collections) {
        return collections.postWithUpvotes.take(this.limit).map(PostsCleanupKeyMapper);
    };
    return PostsResource;
}());
// Spawn a local HTTP server to support reading/writing and creating
// reactive requests.
(0, skip_runtime_1.runWithRESTServer)(new HackerNewsService());
