# tiny-pub-sub
A pub/sub message queue server from scratch in zig, over UDP.

For development, download the SQLite amalgamation from the [SQLite download page](https://www.sqlite.org/download.html) and place the sqlite.c and sqlite.h file in the server/lib/sqlite3/ folder. Then run `zig build run` and possibly replace the hashes inside the server/build.zig.zon file. (TODO: use specific commits of my deps other than `master`.)

Instead of also spinning up a client to test the server, you can just run in the terminal `nc -u 127.0.0.1 9999` and start sending messages (starting with the prefix `SND/ACK/HIS`).

`SND` = send. `ACK` = acknowledge. `HIS` = history.

```bash
SND Here is my first message.
(server response to all subs) EVT id:1 Here is my first message.
HIS
(server response to caller) EVT id:1 Here is my first message.
ACK 1
HIS
(No response from server)
```

TODO:
- Some versions of a good client in different languages
- Encryption via one or more saved keys
- Hash the messages from client to verify that the server ends up getting the correct bits
- Write tests 😁✌️☮️
