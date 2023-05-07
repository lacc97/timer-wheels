const std = @import("std");
const assert = std.debug.assert;

pub const DoubleLinkedList = struct {
    const Self = @This();
    const Node = DoublyLinkedNode;

    hook: Node = .{},

    pub inline fn init(self: *Self) void {
        self.hook.init();
    }

    pub fn is_empty(self: *Self) bool {
        return self.hook.next == &self.hook;
    }

    pub fn prepend(self: *Self, new: *Node) void {
        self.hook.prepend(new);
    }

    pub fn append(self: *Self, new: *Node) void {
        self.hook.append(new);
    }

    pub fn splice_before(self: *Self, to_hook: *Node) void {
        if (!self.is_empty()) {
            _splice(&self.hook, to_hook.prev, to_hook);
            self.init();
        }
    }
    pub fn splice_after(self: *Self, to_hook: *Node) void {
        if (!self.is_empty()) {
            _splice(&self.hook, to_hook, to_hook.next);
            self.init();
        }
    }

    inline fn _splice(list: *Node, prev: *Node, next: *Node) void {
        const first = list.next;
        const last = list.prev;

        first.prev = prev;
        prev.next = first;

        last.next = next;
        next.prev = last;
    }
};

pub const DoublyLinkedNode = struct {
    const Self = @This();

    prev: *DoublyLinkedNode = undefined,
    next: *DoublyLinkedNode = undefined,

    pub fn init(self: *Self) void {
        self.prev = self;
        self.next = self;
    }

    pub fn prepend(self: *Self, new: *Self) void {
        _add(new, self.prev, self);
    }
    pub fn append(self: *Self, new: *Self) void {
        _add(new, self, self.next);
    }

    pub fn unlink(self: *Self) void {
        if (self.next != self) {
            _del_entry(self);
            self.init();
        }
    }

    inline fn _add(new: *Self, prev: *Self, next: *Self) void {
        _add_validate(new, prev, next);

        next.prev = new;
        new.next = next;
        new.prev = prev;
        prev.next = new;
    }
    inline fn _add_validate(new: *Self, prev: *Self, next: *Self) void {
        assert(next.prev == prev);
        assert(prev.next == next);
        assert(new != prev and new != next);
    }

    inline fn _del(prev: *Self, next: *Self) void {
        next.prev = prev;
        prev.next = next;
    }
    inline fn _del_entry(entry: *Self) void {
        _del_entry_validate(entry);
        _del(entry.prev, entry.next);
    }
    inline fn _del_entry_validate(entry: *Self) void {
        const prev = entry.prev;
        const next = entry.next;

        assert(prev != entry);
        assert(next != entry);
        assert(prev.next == entry);
        assert(next.prev == entry);
    }

    inline fn _replace(old: *Self, new: *Self) void {
        new.next = old.next;
        new.next.prev = new;
        new.prev = old.prev;
        new.prev.next = new;
    }

    inline fn _swap(node1: *Self, node2: *Self) void {
        var pos = node2.prev;

        _del_entry(node2);
        _replace(node1, node2);
        if (pos == node1) pos = node2;
        append(pos, node1);
    }
};
