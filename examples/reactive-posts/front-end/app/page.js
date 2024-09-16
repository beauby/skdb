import { jsx as _jsx } from "react/jsx-runtime";
import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App.tsx";
import { connect } from "skdb-ts-thin";
async function init() {
    const conn = await connect("ws://localhost:3586", "865fe079-0ba3-4efa-b027-eff62a0e13c6", {
        "accessKey": "root",
        "privateKey": await crypto.subtle.importKey("raw", Uint8Array.from(atob("41bxagrWX5y/DmzXAi6xeZDYWUjd38LUwWAk9aeBjHQ="), c => c.charCodeAt(0)), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]),
        "deviceUuid": crypto.randomUUID(),
    }, []);
    return conn;
}
init().then((skdb) => {
    window.skdb = skdb;
    ReactDOM.createRoot(document.getElementById("root")).render(_jsx(React.StrictMode, { children: _jsx(App, {}) }));
});
