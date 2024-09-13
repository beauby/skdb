import type {
  SKStore,
  TJSON,
  Mapper,
  EagerCollection,
  NonEmptyIterator,
  SimpleSkipService,
  //  Resource,
} from "skip-runtime";

// import { runWithRESTServer } from "skip-runtime";

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

class HackerNewsService implements SimpleSkipService {
  private inputTables = ["posts", "users", "upvotes"];
  private resources = [PostsResource];

  reactiveCompute(
    store: SKStore,
    inputCollections: Record<string, EagerCollection<TJSON, TJSON>>,
  ): Record<string, EagerCollection<TJSON, TJSON>> {
    const upvotes = (
      inputCollections.upvotes as EagerCollection<string, Upvote>
    ).map(UpvotesMapper);
    const postsWithUpvotes = (
      inputCollections.posts as EagerCollection<string, Post>
    ).map(
      PostsMapper,
      inputCollections.users as EagerCollection<string, User>,
      upvotes,
    );

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

const foo = () => {
  return "bar";
};

class PostsResource {
  // implements Resource {
  constructor(private limit: number) {}

  reactiveCompute(
    store: SKStore,
    collections: Record<string, EagerCollection<TJSON, TJSON>>,
  ): EagerCollection<string, TJSON> {
    foo();
    return (
      collections.postWithUpvotes as EagerCollection<
        [number, string],
        Post & { upvotes: number; author: User }
      >
    )
      .take(this.limit)
      .map(PostsCleanupKeyMapper);
  }
}

// Spawn a local HTTP server to support reading/writing and creating
// reactive requests.
// runWithRESTServer(new HackerNewsService());
