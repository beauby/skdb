import { useEffect, useState } from 'react';
import { SKDB } from 'skdb-ts-thin';

export default function Home({ skdb }) {
  const [posts, setPosts] = useState([]);

  useEffect(() => {
    return skdb.subscribe("posts", (rows) => {
      console.log("Got it", rows);
      // posts.push(...rows);
      setPosts(rows);
    }, (added, removed) => {
      console.log(added, removed);
    }).close;
  }, []);
  return (
    <div>
      <main>
        <ol>{posts.map(p => <li><h3>{p.title}</h3><p>{p.body}</p><p>Likes: {p.likes}</p></li>)}</ol>
      </main>
    </div>
  );
}

// function LikesPerPost() {
//   useEffect(() => {
    
//   }, [])
// }
