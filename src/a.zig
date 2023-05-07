const std = @import("std");
const assert = std.debug.assert;

const List = @import("list.zig").DoubleLinkedList;
const ListNode = @import("list.zig").DoublyLinkedNode;

pub const TimerWheel = struct {
    pub const Self = @This();

    pub const Config = struct {
        levels: u4,

        buckets_per_level_log2: u3,
        granularity_per_level_log2: u3,
    };

    buckets: []Bucket,
    ticks: u64,

    cfg: struct {
        buckets_per_level_log2: u6,
        granularity_per_level_log2: u6,

        levels: usize,

        buckets_per_level: usize,
        granularity_per_level: usize,

        max_lifetime: u64,
    },

    pub fn init(allocator: std.mem.Allocator, config: Config) !Self {
        assert(config.granularity_per_level_log2 <= config.buckets_per_level_log2);
        assert(config.buckets_per_level_log2 > 0);

        // zig fmt: off
        var self = Self{
            .buckets = undefined,
            .ticks = 0,
            .cfg = .{
                .buckets_per_level_log2 = config.buckets_per_level_log2,
                .granularity_per_level_log2 = config.granularity_per_level_log2,
                .levels = config.levels,
                .buckets_per_level = _lvl_size(config.buckets_per_level_log2),
                .granularity_per_level = _lvl_gran(1, config.granularity_per_level_log2),
                .max_lifetime = _wheel_max_lifetime(config.levels, config.buckets_per_level_log2, config.granularity_per_level_log2),
            }
        };
        // zig fmt: on

        const buckets = try allocator.alloc(Bucket, self.cfg.levels * @as(usize, self.cfg.buckets_per_level));
        for (buckets) |*bucket| bucket.list.init();
        self.buckets = buckets;

        return self;
    }
    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        for (self.buckets) |*bucket| bucket.list.hook.unlink();
        allocator.free(self.buckets);
    }

    pub noinline fn schedule(self: *Self, lifetime: usize, timer: *Timer) void {
        var delta = lifetime;
        if (delta > 0) delta -= 1;
        const bucket_index = self.calc_bucket_index(delta);
        timer.hook.unlink();
        self.buckets[bucket_index].list.append(&timer.hook);
    }

    /// Multiple ticks. Returns how many callbacks were run.
    pub fn tick_many(self: *Self, ticks: usize) usize {
        var count: usize = 0;
        for (0..ticks) |_| {
            count += self.tick();
        }
        return count;
    }

    /// Multiple ticks with a maximum limit of timers to run (across all ticks). Returns how many callbacks were actually run.
    pub fn tick_many_with_limit(self: *Self, ticks: usize, limit: usize) usize {
        var remaining = limit;
        for (0..ticks) |_| {
            remaining -= self.tick_with_limit(remaining);
        }
        return limit - remaining;
    }

    /// Single tick. Returns how many callbacks were run.
    pub fn tick(self: *Self) usize {
        return self.tick_with_limit(std.math.maxInt(usize));
    }

    /// Single tick with a maximum limit of timers to run. Returns how many callbacks were actually run.
    pub fn tick_with_limit(self: *Self, limit: usize) usize {
        var current_tick = self.ticks;
        self.ticks += 1;

        var count: usize = 0;

        var lvl: usize = 0;
        var lvl_bucket_index: usize = 0;
        while (lvl < self.cfg.levels) {
            var to_expire = Bucket{};
            to_expire.list.init();

            // Move the timers to a new list (on the stack) so that timers that reschedule themselves on this tick don't break things.
            const bucket_index = lvl_bucket_index + (current_tick & (self.cfg.buckets_per_level - 1));
            self.buckets[bucket_index].list.splice_after(&to_expire.list.hook);

            while (count < limit and !to_expire.list.is_empty()) : (count += 1) {
                const timer = to_expire.pop();
                timer.func(timer);
            }

            // Reschedule timers that couldn't be triggered this tick.
            if (!to_expire.list.is_empty()) {
                to_expire.list.splice_before(&self.buckets[self._reschedule_bucket_index()].list.hook);
            }

            // Have we ticked enough times to roll over to the next level?
            if (current_tick & (self.cfg.granularity_per_level - 1) != 0) {
                break;
            }

            lvl += 1;
            lvl_bucket_index += self.cfg.buckets_per_level;
            current_tick >>= self.cfg.granularity_per_level_log2;
        }

        return count;
    }

    // The bucket on the first level that is next to be expired. This function only makes sense when called inside the `tick` family of functions.
    fn _reschedule_bucket_index(self: *Self) usize {
        return self.ticks & (self.cfg.buckets_per_level - 1);
    }

    fn calc_bucket_index(self: *Self, delta: u64) usize {
        var lvl: u8 = 0;
        var lvl_start: u64 = self.cfg.buckets_per_level - 1;
        while (delta >= lvl_start) {
            lvl += 1;
            lvl_start <<= self.cfg.granularity_per_level_log2;

            if (lvl == self.cfg.levels) {
                // We have gone past the max lifetime of the wheel.
                return self._cutoff_bucket_index();
            }
        }

        return self.calc_index(lvl, self.ticks + delta);
    }
    fn _cutoff_bucket_index(self: *Self) usize {
        @setCold(true);
        return self.calc_index(@intCast(u8, self.cfg.levels - 1), self.ticks + self.cfg.max_lifetime);
    }
    inline fn calc_index(self: *Self, lvl: u8, abs_tick: u64) usize {
        const shift = _lvl_shift(@intCast(u4, lvl), @intCast(u3, self.cfg.granularity_per_level_log2));
        const abs_index = (abs_tick >> shift) + 1;
        const index = lvl * self.cfg.buckets_per_level + @intCast(usize, abs_index & (self.cfg.buckets_per_level - 1));
        return index;
    }

    inline fn _wheel_max_lifetime(lvls: u4, lvl_size_log2: u3, rgran_log2: u3) u64 {
        // Cutoff is the start tick for the level past the end.
        const cutoff = _lvl_start_lifetime(lvls, lvl_size_log2, rgran_log2);

        // The max possible lifetime is one less unit of granularity for the last level.
        return (cutoff - _lvl_gran(lvls - 1, rgran_log2));
    }
    inline fn _lvl_start_lifetime(lvl: u4, lvl_size_log2: u3, rgran_log2: u3) u64 {
        if (lvl == 0) return 0;
        return (@as(u64, _lvl_size(lvl_size_log2) - 1)) << (@as(u6, lvl - 1) * rgran_log2);
    }
    inline fn _lvl_gran(lvl: u4, rgran_log2: u3) u64 {
        return @as(u64, 1) << _lvl_shift(lvl, rgran_log2);
    }
    inline fn _lvl_shift(lvl: u4, rgran_log2: u3) u6 {
        return @as(u6, lvl) * @as(u6, rgran_log2);
    }
    inline fn _lvl_offs(lvl: u4, lvl_size_log2: u3) usize {
        return @as(usize, lvl) * (@as(usize, 1) << lvl_size_log2);
    }
    inline fn _lvl_size(lvl_size_log2: u3) usize {
        return @as(usize, 1) << lvl_size_log2;
    }
};

const Bucket = struct {
    const Self = @This();

    list: List = .{},

    pub fn pop(self: *Self) *Timer {
        assert(!self.list.is_empty());

        const popped = self.list.hook.next;
        popped.unlink();
        return @fieldParentPtr(Timer, "hook", popped);
    }
};

pub const Timer = struct {
    hook: ListNode = .{},
    func: *const fn (self: *Timer) void,

    pub fn init(self: *Timer) void {
        self.hook.init();
    }

    pub fn cancel(self: *Timer) void {
        self.hook.unlink();
    }
};

const TimerIncreaseCount = struct {
    const Self = @This();

    timer: Timer = .{ .func = increase },

    count: usize = 0,

    fn increase(self_timer: *Timer) void {
        const self = @fieldParentPtr(Self, "timer", self_timer);

        self.count += 1;
    }
};

test "no hierarchy" {
    const testing = std.testing;

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var timers = try TimerWheel.init(allocator, .{
        .levels = 1,
        .buckets_per_level_log2 = 5,
        .granularity_per_level_log2 = 3,
    });
    defer timers.deinit(allocator);

    var t = TimerIncreaseCount{};
    t.timer.init();

    // unscheduled timer does nothing
    try testing.expectEqual(@as(usize, 0), timers.tick_many(10));
    try testing.expectEqual(@as(usize, 0), t.count);

    // scheduled timer triggers at the right time
    timers.schedule(5, &t.timer);
    try testing.expectEqual(@as(usize, 1), timers.tick_many(6));
    try testing.expectEqual(@as(usize, 1), t.count);

    // timer only gets triggered once
    try testing.expectEqual(@as(usize, 0), timers.tick_many(33));

    // we can reschedule timers
    timers.schedule(5, &t.timer);
    _ = timers.tick_many(6);
    try testing.expectEqual(@as(usize, 2), t.count);

    // we can cancel timers
    timers.schedule(5, &t.timer);
    t.timer.cancel();
    _ = timers.tick_many(6);
    try testing.expectEqual(@as(usize, 2), t.count);

    // rescheduled timers only run on the last scheduled time
    timers.schedule(5, &t.timer);
    timers.schedule(10, &t.timer);
    _ = timers.tick_many(6);
    try testing.expectEqual(@as(usize, 2), t.count);
    _ = timers.tick_many(5);
    try testing.expectEqual(@as(usize, 3), t.count);

    // large time values are bounded
    timers.schedule(256, &t.timer);
    _ = timers.tick_many(32);
    try testing.expectEqual(@as(usize, 4), t.count);
}

test "hierarchy" {
    const testing = std.testing;

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var timers = try TimerWheel.init(allocator, .{
        .levels = 3,
        .buckets_per_level_log2 = 5, // each level contains 32 buckets
        .granularity_per_level_log2 = 3, // each level unit is 8 times the lower level unit
    });
    defer timers.deinit(allocator);

    var t = TimerIncreaseCount{};
    t.timer.init();
    defer t.timer.cancel();

    // scheduled timer may trigger later but not earlier
    timers.schedule(32, &t.timer);
    try testing.expectEqual(@as(usize, 0), timers.tick_many(32));
    try testing.expectEqual(@as(usize, 0), t.count);
    try testing.expectEqual(@as(usize, 1), timers.tick_many(8));
    try testing.expectEqual(@as(usize, 1), t.count);

    timers.schedule(40, &t.timer);
    try testing.expectEqual(@as(usize, 0), timers.tick_many(40));
    try testing.expectEqual(@as(usize, 1), t.count);
    try testing.expectEqual(@as(usize, 1), timers.tick_many(8));
    try testing.expectEqual(@as(usize, 2), t.count);

    timers.schedule(256, &t.timer);
    try testing.expectEqual(@as(usize, 0), timers.tick_many(256));
    try testing.expectEqual(@as(usize, 2), t.count);
    try testing.expectEqual(@as(usize, 1), timers.tick_many(64));
    try testing.expectEqual(@as(usize, 3), t.count);

    timers.schedule(320, &t.timer);
    try testing.expectEqual(@as(usize, 0), timers.tick_many(320));
    try testing.expectEqual(@as(usize, 3), t.count);
    try testing.expectEqual(@as(usize, 1), timers.tick_many(64));
    try testing.expectEqual(@as(usize, 4), t.count);

    timers.schedule(38, &t.timer);
    try testing.expectEqual(@as(usize, 0), timers.tick_many(38));
    try testing.expectEqual(@as(usize, 4), t.count);
    try testing.expectEqual(@as(usize, 1), timers.tick_many(8));
    try testing.expectEqual(@as(usize, 5), t.count);

    timers.schedule(316, &t.timer);
    try testing.expectEqual(@as(usize, 0), timers.tick_many(316));
    try testing.expectEqual(@as(usize, 5), t.count);
    try testing.expectEqual(@as(usize, 1), timers.tick_many(64));
    try testing.expectEqual(@as(usize, 6), t.count);

    timers.schedule(308, &t.timer);
    try testing.expectEqual(@as(usize, 0), timers.tick_many(308));
    try testing.expectEqual(@as(usize, 6), t.count);
    try testing.expectEqual(@as(usize, 0), timers.tick_many(8));
    try testing.expectEqual(@as(usize, 6), t.count);
    try testing.expectEqual(@as(usize, 1), timers.tick_many(56));
    try testing.expectEqual(@as(usize, 7), t.count);
}
