const std = @import("std");
const network = @import("network");
const zqlite = @import("zqlite");

// test by running this from any machine on the network
// nc -u 127.0.0.1 9999

const buflen = 4096;
const Global = struct {
    connected_clients: [64]network.EndPoint = undefined,
    connected_client_count: usize = 0,
    clients_lock: std.Thread.Mutex = .{},

    event_buf: [buflen]u8 = undefined,
    event_len: usize = 0,
    event_id: i64 = 0,
    event_buf_lock: std.Thread.Mutex = .{},
    event_cond: std.Thread.Condition = .{},
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    try network.init();
    defer network.deinit();

    // Start up sqlite
    var sqlite_pool = try zqlite.Pool.init(allocator, .{
        .path = "./db.sqlite",
        .on_first_connection = initializeDB,
    });

    // Create a UDP socket
    var sock = try network.Socket.create(.ipv4, .udp);
    defer sock.close();

    // Create our global data
    var global = Global{};

    // Allow port re-use so that multiple instances
    // of this program can all subscribe to the UDP broadcasts
    try sock.enablePortReuse(true);
    const incoming_endpoint = network.EndPoint{
        .address = network.Address{ .ipv4 = network.Address.IPv4.init(127, 0, 0, 1) },
        .port = 9999,
    };
    sock.bind(incoming_endpoint) catch |err| {
        std.debug.print("failed to bind to {}: {}\n", .{ incoming_endpoint, err });
        return;
    };

    std.debug.print("Waiting for UDP messages from socket {!}\n", .{sock.getLocalEndPoint()});

    // Start the broadcaster
    const broadcaster_thread = try std.Thread.spawn(.{}, broadcastToClients, .{ &global, sock });
    defer broadcaster_thread.join();

    // Make a conn for the main thread
    const c1 = sqlite_pool.acquire();
    defer sqlite_pool.release(c1);

    loadClientsFromDB(&global, c1);
    sendAllSavedUnreadEvents(&global, c1, sock, false, undefined);

    // Start the receiver
    var msg: [buflen]u8 = undefined;
    while (true) {
        // receive ping
        const receiveFrom = try sock.receiveFrom(msg[0..buflen]);
        const sender = receiveFrom.sender;
        addToClientListIfNotAlready(&global, c1, sender);

        // If client is sending SND => is sending a new event
        if (std.mem.eql(u8, msg[0..4], "SND ")) {
            const length_to_copy = buflen - 4;
            var msg_minus_ctrl_scheme: [buflen]u8 = undefined;
            std.mem.copyForwards(u8, msg_minus_ctrl_scheme[0..length_to_copy], msg[4..]);
            writeEventAndQueueForSending(&global, c1, sender, msg_minus_ctrl_scheme, receiveFrom.numberOfBytes - 4);

            // If client is sending ACK => is marking an event read
        } else if (std.mem.eql(u8, msg[0..4], "ACK ")) {
            const length_to_copy = buflen - 4;
            var msg_minus_ctrl_scheme: [buflen]u8 = undefined;
            std.mem.copyForwards(u8, msg_minus_ctrl_scheme[0..length_to_copy], msg[4..]);
            markEventReadByIdStr(sock, sender, c1, &msg_minus_ctrl_scheme, receiveFrom.numberOfBytes - 4);
        } else if (std.mem.eql(u8, msg[0..3], "HIS")) {
            sendAllSavedUnreadEvents(&global, c1, sock, true, sender);
        } else {
            _ = sock.sendTo(sender, "ERR Wrong format. Usage: 'SND xyz' 'ACK 4'\n") catch |err| {
                std.debug.print("Failed to send to {s}: {}\n", .{ sender, err });
                continue;
            };
        }
    }
}

fn writeEventAndQueueForSending(global: *Global, c1: zqlite.Conn, client: network.EndPoint, msg: [buflen]u8, msg_len: usize) void {
    // Write to WAL
    c1.transaction() catch |err| {
        std.debug.print("Failed to start transaction before writing event to db: {}\n", .{err});
        return;
    };
    var msg_len_with_end_popped = msg_len;
    if (msg[msg_len] == std.ascii.whitespace[2] or msg[msg_len] == 170) {
        msg_len_with_end_popped -= 1;
    }
    var trimmed_msg = trimFromBufStr(&msg, msg_len);
    c1.exec("INSERT INTO queue_history (msg, sender_ip, sender_port) values (?1, ?2, ?3)", .{ trimmed_msg.str[0..trimmed_msg.str_len], &client.address.ipv4.value, client.port }) catch |err| {
        std.debug.print("Failed to write msg into db: {}\n", .{err});
        return;
    };
    const event_id = c1.lastInsertedRowId();
    c1.commit() catch |err| {
        std.debug.print("Failed commit msg into db: {}\n", .{err});
        return;
    };

    // add event to be sent out
    global.event_buf_lock.lock();
    global.event_buf = trimmed_msg.str;
    global.event_len = trimmed_msg.str_len;
    global.event_id = event_id;
    _ = global.event_cond.signal();
    global.event_buf_lock.unlock();
}

fn markEventReadByIdStr(sock: network.Socket, client: network.EndPoint, c1: zqlite.Conn, msg_id_str: *[buflen]u8, msg_len: usize) void {
    var trimmed_msg = trimFromBufStr(msg_id_str, msg_len);
    const msg_id: i64 = std.fmt.parseInt(i64, trimmed_msg.str[0..trimmed_msg.str_len], 10) catch {
        _ = sock.sendTo(client, "ERR Failed to parse id as int\n") catch |send_err| {
            std.debug.print("Failed to send to {s}: {}\n", .{ client, send_err });
            return;
        };
        return;
    };
    c1.exec("UPDATE queue_history SET read_count=read_count+1 WHERE id=?1", .{msg_id}) catch |err| {
        std.debug.print("Failed to write msg into db: {}\n", .{err});
        return;
    };
}

fn sendEventToClient(sock: network.Socket, client: network.EndPoint, msg: [buflen]u8, msg_len: usize) void {
    _ = sock.sendTo(client, msg[0..msg_len]) catch |err| {
        std.debug.print("Failed to send to {s}: {}\n", .{ client, err });
    };
}

fn broadcastToClients(global: *Global, sock: network.Socket) void {
    while (true) {
        var msg: [buflen]u8 = undefined;
        var msg_len: usize = 0;
        var msg_id: i64 = 0;

        global.event_buf_lock.lock();
        while (global.event_len == 0) {
            _ = global.event_cond.wait(&global.event_buf_lock);
        }
        msg = global.event_buf;
        msg_len = global.event_len;
        msg_id = global.event_id;
        global.event_buf_lock.unlock();

        var msg_plus_ctrl_scheme: [buflen]u8 = undefined;
        const msg_len_plus_ctrl_scheme: usize = msg_len + 8 + countDigits(msg_id);
        _ = std.fmt.bufPrint(&msg_plus_ctrl_scheme, "EVT id:{d} {s}", .{ msg_id, msg[0..msg_len] }) catch |err| {
            std.debug.print("Failed parse msg out: {}\n", .{err});
            return;
        };

        global.clients_lock.lock();
        for (0..global.connected_client_count) |i| {
            sendEventToClient(sock, global.connected_clients[i], msg_plus_ctrl_scheme, msg_len_plus_ctrl_scheme);
        }
        global.clients_lock.unlock();

        global.event_buf_lock.lock();
        global.event_buf = undefined;
        global.event_len = 0;
        global.event_id = 0;
        global.event_buf_lock.unlock();
    }
}

fn addToClientListIfNotAlready(global: *Global, c1: zqlite.Conn, client: network.EndPoint) void {
    var do_we_know_client: bool = false;
    for (0..(global.connected_client_count + 1)) |i| {
        if (std.mem.eql(u8, &global.connected_clients[i].address.ipv4.value, &client.address.ipv4.value) and global.connected_clients[i].port == client.port) {
            do_we_know_client = true;
            break;
        }
    }
    if (!do_we_know_client) {
        global.clients_lock.lock();
        c1.exec("INSERT INTO clients (ip, port) values (?1, ?2)", .{ &client.address.ipv4.value, client.port }) catch |err| {
            std.debug.print("Failed to insert client into db: {}\n", .{err});
            return;
        };
        global.connected_clients[global.connected_client_count] = client;
        global.connected_client_count += 1;
        global.clients_lock.unlock();
    }
}

fn loadClientsFromDB(global: *Global, c1: zqlite.Conn) void {
    var rows = c1.rows("SELECT ip, port FROM clients", .{}) catch |err| {
        std.debug.print("Failed to grab saved clients for population: {}\n", .{err});
        return;
    };
    defer rows.deinit();

    var tmp_endpoint: network.EndPoint = undefined;
    var ipv4_buf: [20]u8 = undefined;
    var ipv4_len: usize = 0;
    var i: u32 = 0;
    global.clients_lock.lock();
    while (rows.next()) |row| {
        std.mem.copyForwards(u8, &ipv4_buf, row.text(0));
        ipv4_len = row.textLen(0);
        if ((std.mem.indexOfScalar(u8, &ipv4_buf, ':') orelse 0) == 0) {
            std.mem.copyForwards(u8, &ipv4_buf, "127.0.0.1");
            ipv4_len = 9;
        }
        tmp_endpoint.address.ipv4 = network.Address.IPv4.parse(ipv4_buf[0..ipv4_len]) catch |err| {
            std.debug.print("Couldn't parse endpoint: {}", .{err});
            return;
        };
        tmp_endpoint.port = @as(u16, @intCast(row.int(1)));
        std.debug.print("Loading back client {s}\n", .{tmp_endpoint});

        global.connected_clients[i] = tmp_endpoint;
        global.connected_client_count += 1;
        i += 1;
    }
    global.clients_lock.unlock();
}

fn sendAllSavedUnreadEvents(global: *Global, c1: zqlite.Conn, sock: network.Socket, should_send_to_specific_client: bool, specific_client: network.EndPoint) void {
    var rows = c1.rows("SELECT id, msg FROM queue_history WHERE read_count=0", .{}) catch |err| {
        std.debug.print("Failed to grab queue_history: {}\n", .{err});
        return;
    };
    defer rows.deinit();

    var msg_id: i64 = 0;
    while (rows.next()) |row| {
        var msg_plus_ctrl_scheme: [buflen]u8 = undefined;
        // minimum 9 with the "EVT ", etc.
        var msg_len_plus_ctrl_scheme: usize = 8;
        msg_id = row.int(0);
        _ = std.fmt.bufPrint(&msg_plus_ctrl_scheme, "EVT id={d} {s}", .{ msg_id, row.text(1) }) catch |err| {
            std.debug.print("Failed parse msg out: {}\n", .{err});
            return;
        };
        msg_len_plus_ctrl_scheme += row.textLen(1) + countDigits(msg_id);

        if (should_send_to_specific_client) {
            sendEventToClient(sock, specific_client, msg_plus_ctrl_scheme, msg_len_plus_ctrl_scheme);
        } else {
            global.clients_lock.lock();
            for (0..global.connected_client_count) |i| {
                sendEventToClient(sock, global.connected_clients[i], msg_plus_ctrl_scheme, msg_len_plus_ctrl_scheme);
            }
            global.clients_lock.unlock();
        }
    }
}

fn initializeDB(conn: zqlite.Conn) !void {
    try conn.execNoArgs("CREATE TABLE IF NOT EXISTS clients(id INTEGER PRIMARY KEY AUTOINCREMENT, initiated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, ip TEXT NOT NULL, port INTEGER NOT NULL)");
    try conn.execNoArgs("CREATE TABLE IF NOT EXISTS queue_history(id INTEGER PRIMARY KEY AUTOINCREMENT, received_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, msg TEXT NOT NULL, read_count INTEGER NOT NULL DEFAULT 0, sender_ip TEXT NOT NULL, sender_port INTEGER NOT NULL)");
}

fn countDigits(num: i64) usize {
    var count: usize = 0;
    var n = num;
    if (n == 0) return 1;
    while (n != 0) {
        // n /= 10;
        n = @divFloor(n, 10);
        count += 1;
    }
    return count;
}

fn trimFromBufStr(str: *const [buflen]u8, str_len: usize) struct { str: [buflen]u8, str_len: usize } {
    var tmp: [buflen]u8 = undefined;
    if (str_len == 0) {
        return .{ .str = tmp, .str_len = str_len };
    }
    const new_str_len: usize = str_len;
    var shift_forward_how_many: usize = 0;
    var i: usize = 0;
    while (true) : (i += 1) {
        if ((std.mem.indexOf(u8, &std.ascii.whitespace, &[_]u8{str[i]}) orelse 9999) != 9999) {
            shift_forward_how_many += 1;
        } else {
            break;
        }
    }

    var pop_off_how_many: usize = 0;
    var j: usize = new_str_len - 1;
    while (true) : (j -= 1) {
        if ((std.mem.indexOf(u8, &std.ascii.whitespace, &[_]u8{str[j]}) orelse 9999) != 9999) {
            pop_off_how_many += 1;
        } else {
            break;
        }
    }

    var k: usize = shift_forward_how_many;
    var l: usize = 0;
    while (k <= (str_len - pop_off_how_many)) : (k += 1) {
        tmp[l] = str[k];
        l += 1;
    }
    return .{ .str = tmp, .str_len = (str_len - pop_off_how_many - shift_forward_how_many) };
}
