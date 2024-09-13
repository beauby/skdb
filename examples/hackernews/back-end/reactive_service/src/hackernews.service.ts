import type {
  SKStore,
  TJSON,
  Mapper,
  EagerCollection,
  NonEmptyIterator,
  SimpleSkipService,
  Resource,
} from "skip-runtime";

import { runWithRESTServer } from "skip-runtime";

interface Post {
  id: number;
  author_id: number;
  title: string;
  url: string;
  body: string;
};

interface User {
  id: number;
  name: string;
  email: string;
};

interface Upvote {
  id: number;
  post_id: number;
  user_id: number;
};

class HackerNewsService implements SimpleSkipService {
  private inputTables = ["posts", "users", "upvotes"];
  private resources = [PostsResource];

  reactiveCompute(
    store: SKStore,
    inputCollections: Record<string, EagerCollection<string, TJSON>>,
  ): Record<string, EagerCollection<TJSON, TJSON>> {
    const upvotes = inputCollections.upvotes.map(UpvotesMapper);
    const postsWithUpvotes = inputCollections.posts.map(
      PostsMapper,
      inputCollections.users,
      upvotes,
    );

    return {
      postsWithUpvotes,
    };
  }
}

class UpvotesMapper implements Mapper<string, Upvote, number, string> {
  mapElement(
    key: string,
    it: NonEmptyIterator<{ post_id: number }>,
  ): Iterable<[number, string]> {
    const value = it.first().post_id;
    return new Array([value, key]);
  }
}

class PostsMapper implements Mapper<string, TJSON, TJSON, TJSON> {
  constructor(
    private users: EagerCollection<string, TJSON>,
    private upvotes: EagerCollection<string, TJSON>,
  ) {}

  mapElement(
    key: string,
    it: NonEmptyIterator<TJSON>,
  ): Iterable<[[number, string], TJSON]> {
    const post = it.first();
    const upvotes = this.upvotes.getArray(key).length;
    const author = this.users.getOne(post.author_id);
    return new Array([[-upvotes, key], { ...post, upvotes, author }]);
  }
}

class PostsCleanupKeyMapper implements Mapper<TJSON, TJSON, string, TJSON> {
  mapElement(
    key: [number, string],
    it: NonEmptyIterator<TJSON>,
  ): Iterable<[string, TJSON]> {
    const post = it.first();
    return new Array([key[1], post]);
  }
}

class PostsResource implements Resource {
  constructor(private limit: number) {}

  reactiveCompute(
    store: SKStore,
    collections: Record<string, EagerCollection<TJSON, TJSON>>,
  ): EagerCollection<string, TJSON> {
    return collections.postWithUpvotes
      .take(this.limit)
      .map(PostsCleanupKeyMapper);
  }
}

// Spawn a local HTTP server to support reading/writing and creating
// reactive requests.
runWithRESTServer(new HackerNewsService());
