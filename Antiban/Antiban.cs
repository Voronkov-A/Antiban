using System;
using System.Collections.Generic;
using System.Linq;

namespace Antiban
{
    public class Antiban
    {
        private const int BroadcastPriority = 1;
        private static readonly TimeSpan GlobalInterval = TimeSpan.FromSeconds(10);
        private static readonly TimeSpan SamePhoneInterval = TimeSpan.FromMinutes(1);
        private static readonly TimeSpan SamePhoneBroadcastInterval = TimeSpan.FromHours(24);

        private readonly ScheduledItemQueue _allMessages;
        private readonly Dictionary<string, LocalQueues> _localQueues;

        public Antiban()
        {
            _allMessages = new ScheduledItemQueue(GlobalInterval);
            _localQueues = new Dictionary<string, LocalQueues>();
        }

        /// <summary>
        /// Добавление сообщений в систему, для обработки порядка сообщений
        /// </summary>
        /// <param name="eventMessage"></param>
        public void PushEventMessage(EventMessage eventMessage)
        {
            var queues = DetectQueuesToSchedule(eventMessage);
            var item = new ScheduledItem(eventMessage.DateTime, eventMessage);
            var scheduledItem = item;

            do
            {
                item = scheduledItem;
                scheduledItem = Schedule(item, queues);
            } while (scheduledItem != item);

            Add(scheduledItem, queues);
        }

        /// <summary>
        /// Вовзращает порядок отправок сообщений
        /// </summary>
        /// <returns></returns>
        public List<AntibanResult> GetResult()
        {
            return _allMessages.Items.Select(x => new AntibanResult()
            {
                EventMessageId = x.Message.Id,
                SentDateTime = x.TargetTime
            }).ToList();
        }

        private List<ScheduledItemQueue> DetectQueuesToSchedule(EventMessage message)
        {
            var localQueues = GetOrCreateLocalQueues(message);

            var queuesToSchedule = new List<ScheduledItemQueue>(3);

            if (message.Priority == BroadcastPriority)
            {
                queuesToSchedule.Add(localQueues.BroadcastMessages);
            }

            queuesToSchedule.Add(localQueues.AllMessages);
            queuesToSchedule.Add(_allMessages);
            return queuesToSchedule;
        }

        private LocalQueues GetOrCreateLocalQueues(EventMessage eventMessage)
        {
            if (!_localQueues.TryGetValue(eventMessage.Phone, out var localQueues))
            {
                localQueues = new LocalQueues();
                _localQueues[eventMessage.Phone] = localQueues;
            }

            return localQueues;
        }

        private static ScheduledItem Schedule(ScheduledItem item, IEnumerable<ScheduledItemQueue> queues)
        {
            return queues.Aggregate(item, (x, queue) => queue.Schedule(x));
        }

        private static void Add(ScheduledItem item, IEnumerable<ScheduledItemQueue> queues)
        {
            foreach (var queue in queues)
            {
                queue.Add(item);
            }
        }

        private class LocalQueues
        {
            public ScheduledItemQueue AllMessages { get; } = new(SamePhoneInterval);

            public ScheduledItemQueue BroadcastMessages { get; } = new(SamePhoneBroadcastInterval);
        }

        private record ScheduledItem(DateTime TargetTime, EventMessage Message);

        private class ScheduledItemComparer : IComparer<ScheduledItem>
        {
            private readonly TimeSpan _interval;

            public ScheduledItemComparer(TimeSpan interval)
            {
                _interval = interval;
            }

            public int Compare(ScheduledItem? x, ScheduledItem? y)
            {
                switch (x, y)
                {
                    case (null, not null):
                        return -1;
                    case (not null, null):
                        return 1;
                    case (null, null):
                        return 0;
                    default:
                    {
                        var diff = x.TargetTime - y.TargetTime;

                        if (diff <= -_interval)
                        {
                            return -1;
                        }

                        return diff >= _interval ? 1 : 0;
                    }
                }
            }
        }

        private class ScheduledItemQueue
        {
            private readonly TimeSpan _interval;
            private readonly SortedSet<ScheduledItem> _items;

            public ScheduledItemQueue(TimeSpan interval)
            {
                _interval = interval;
                _items = new SortedSet<ScheduledItem>(new ScheduledItemComparer(interval));
            }

            public IReadOnlyCollection<ScheduledItem> Items => _items;

            public ScheduledItem Schedule(ScheduledItem time)
            {
                while (_items.TryGetValue(time, out var existingTime))
                {
                    time = new ScheduledItem(existingTime.TargetTime.Add(_interval), time.Message);
                }

                return time;
            }

            public void Add(ScheduledItem item)
            {
                if (!_items.Add(item))
                {
                    throw new ArgumentException("Duplicate detected.", nameof(item));
                }
            }
        }
    }
}
