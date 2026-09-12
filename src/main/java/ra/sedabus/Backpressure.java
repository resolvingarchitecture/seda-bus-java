package ra.sedabus;

/**
 * Controls what a channel does when {@link SEDAMessageChannel#send} is
 * called against a full queue.
 *
 * <p>Every previous release of this channel had exactly one behaviour here -
 * {@link #Reject} - with no way to configure anything else, found by an
 * independent production-readiness audit as a real functional gap next to
 * every other {@code seda-bus} language port, which all support this same
 * four-way choice.
 */
public enum Backpressure {
    /** The producer blocks (up to the send timeout, if any) until there is room. */
    Block,

    /** {@code send} returns {@code false} immediately when the queue is full. */
    Reject,

    /** Silently discard the envelope being sent. */
    DropNewest,

    /** Evict the oldest queued envelope to make room. */
    DropOldest,
}
