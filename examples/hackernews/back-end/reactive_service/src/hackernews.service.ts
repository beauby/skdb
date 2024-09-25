import type {
  SKStore,
  TJSON,
  Mapper,
  EagerCollection,
  NonEmptyIterator,
  SkipService,
  Resource,
  Param,
} from "skip-runtime";

import { runService } from "skip-runtime";

type Post = {
  author_id: number;
  title: string;
  url: string;
  body: string;
};

type User = {
  name: string;
  email: string;
};

type Upvote = {
  post_id: number;
  user_id: number;
};

class HackerNewsService implements SkipService {
  inputCollections = ["posts", "users", "upvotes"];
  resources = { posts: PostsResource };

  reactiveCompute(
    _store: SKStore,
    inputCollections: {
      posts: EagerCollection<string, Post>,
      users: EagerCollection<string, User>,
      upvotes: EagerCollection<string, Upvote>,
    },
  ): Record<string, EagerCollection<TJSON, TJSON>> {
    console.log(inputCollections);
    const upvotes = inputCollections.upvotes.map(UpvotesMapper);
    const postsWithUpvotes = inputCollections.posts 
      .map(PostsMapper, inputCollections.users, upvotes);

    return {
      postsWithUpvotes,
    };
  }
}

class UpvotesMapper {
  mapElement(
    key: string,
    it: NonEmptyIterator<Upvote>,
  ): Iterable<[number, string]> {
    const value = it.first().post_id;
    return [[value, key]];
  }
}

class PostsMapper {
  constructor(
    private users: EagerCollection<string, User>,
    private upvotes: EagerCollection<number, string>,
  ) {}

  mapElement(
    key: string,
    it: NonEmptyIterator<Post>,
  ): Iterable<[[number, string], Post & { upvotes: number; author: User }]> {
    const post = it.first();
    const upvotes = this.upvotes.getArray(Number(key)).length;
    const author = this.users.getOne(post.author_id.toString());
    return [[[-upvotes, key], { ...post, upvotes, author }]];
  }
}

class PostsCleanupKeyMapper {
  mapElement(
    key: [number, string],
    it: NonEmptyIterator<TJSON>,
  ): Iterable<[string, TJSON]> {
    const post = it.first();
    return [[key[1], post]];
  }
}

class PostsResource implements Resource {
  private limit: number;
  
  constructor(limit: Param, ...args: Param[]) {
    // console.log(limit);
    // console.log(args);
    this.limit = limit as number;
  }

  reactiveCompute(
    _store: SKStore,
    collections: {
      postsWithUpvotes: EagerCollection<[number, string], Post & { upvotes: number; author: User }>
    },
  ): EagerCollection<string, TJSON> {
    // console.log("limit", this.limit);
    return collections.postsWithUpvotes 
      .take(this.limit)
      .map(PostsCleanupKeyMapper);
  }
}

// Spawn a local HTTP server to support reading/writing and creating
// reactive requests.
runService(new HackerNewsService(), 8080);
