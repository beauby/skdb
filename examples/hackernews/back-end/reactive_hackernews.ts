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

class HackerNewsService implements SimpleSkipService {
  private inputTables = ["posts", "users", "upvotes"],
  private resources = [PostsResource]

  reactiveCompute(
    store: SKStore,
    inputCollections: Record<string, EagerCollection<string, TJSON>>,
  ): Record<string, EagerCollection<TJSON, TJSON>> {
    const upvotes = inputCollections.upvotes.map(UpvotesMapper);
    const postsWithUpvotes = inputCollections.posts.map(PostsMapper, inputCollections.users, upvotes);

    return {
      postsWithUpvotes
    }
  }
}

class UpvotesMapper implements Mapper<string, TJSON, string, TJSON> {
  mapElement(key: string, it: NonEmptyIterator<TJSON>): Iterable<[string, TJSON]> {
    const value = it.first().post_id;
    return [[value, key]]
  }
}

class PostsMapper implements Mapper<string, TJSON, TJSON, TJSON> {
  constructor(
    private users: EagerColelction<string, TJSON>,
    private upvotes: EagerCollection<string, TJSON>
  ) {}
  
  mapElement(key: string, it: NonEmptyIterator<TJSON>): Iterable<[[number, string], TJSON]> {
    const post = it.first();
    const upvotes = this.upvotes.getArray(key).length;
    const author = this.users.getOne(post.author_id);
    return [[[-upvotes, key], { ...post, upvotes, author }]]
  }
}

class PostsCleanupKeyMapper implements Mapper<TJSON, TJSON, string, TJSON> {
  mapElement(key: [number, string], it: NonEmptyIterator<TJSON>): Iterable<[string, TJSON]> {
    const post = it.first();
    return [[key[1], post]];
  }
}

class PostsResource implements Resource {
  constructor(private limit: number) {}
  
  reactiveCompute(
    store: SKStore,
    collections: Record<string, EagerCollection<TJSON, TJSON>>
  ): EagerCollection<string, TJSON> {
    return collections.postWithUpvotes.take(this.limit).map(PostsCleanupKeyMapper);
  }
}

runWithRESTServer(new HackerNewsService());
