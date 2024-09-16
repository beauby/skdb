import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App.tsx";
import { connect } from "skdb-ts-thin";

async function connectHelper(mirror: [string]): SKDB {
  return await connect(
    "ws://localhost:3586",
    "865fe079-0ba3-4efa-b027-eff62a0e13c6",
    {
      "accessKey": "root",
      "privateKey": await crypto.subtle.importKey(
        "raw",
        Uint8Array.from(
          atob(
            "41bxagrWX5y/DmzXAi6xeZDYWUjd38LUwWAk9aeBjHQ="
          ), c => c.charCodeAt(0)),
        { name: "HMAC", hash: "SHA-256" },
        false,
        ["sign"],
      ),
      "deviceUuid": crypto.randomUUID(),
    },
    mirror.map(table => ({table})));
}

async function init() {
  const conn = await connectHelper([]);

  await conn.exec("CREATE TABLE IF NOT EXISTS posts (id INTEGER, title TEXT, body TEXT)");
  // await conn.exec("INSERT INTO posts VALUES(1, 'hello', 'it works')");

  return await connectHelper(["posts"]);
}

init().then((skdb) => {
  window.skdb = skdb;
  ReactDOM.createRoot(document.getElementById("root")!).render(
    <React.StrictMode>
      <App skdb={skdb} />
    </React.StrictMode>,
  );
});

// export default function Home() {
//   return (
//     <div>
//       <main>
//         <ol>
//           <li>
//             Get started by editing <code>app/page.tsx</code>.
//           </li>
//           <li>Save and see your changes instantly.</li>
//         </ol>
//       </main>
//     </div>
//   );
// }
