import { MATCH_STATUS } from '../validation/matches.js';

/**
 * Determine the current match status based on the provided start and end times.
 * @param {string|number|Date} startTime - The match start time (Date or value parseable by `Date`).
 * @param {string|number|Date} endTime - The match end time (Date or value parseable by `Date`).
 * @param {Date} [now=new Date()] - Reference time used to evaluate status; defaults to the current time.
 * @returns {string|null} One of `MATCH_STATUS.SCHEDULED`, `MATCH_STATUS.LIVE`, or `MATCH_STATUS.FINISHED`; returns `null` if `startTime` or `endTime` cannot be parsed as valid dates.
 */
export function getMatchStatus(startTime, endTime, now = new Date()) {
    const start = new Date(startTime);
    const end = new Date(endTime);

    if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) {
        return null;
    }

    if (now < start) {
        return MATCH_STATUS.SCHEDULED;
    }

    if (now >= end) {
        return MATCH_STATUS.FINISHED;
    }

    return MATCH_STATUS.LIVE;
}

/**
 * Ensure a match object's status matches the computed status and apply an update if it differs.
 *
 * @param {Object} match - Match object containing timing and status fields.
 * @param {string|Date|number} match.startTime - Match start time (Date, ISO string, or epoch) used to compute status.
 * @param {string|Date|number} match.endTime - Match end time (Date, ISO string, or epoch) used to compute status.
 * @param {string} match.status - Current status value on the match object; may be updated by this function.
 * @param {(newStatus: string) => Promise<void>} updateStatus - Async function invoked with the new status when a change is required.
 * @returns {string} The match's status after synchronization.
 */
export async function syncMatchStatus(match, updateStatus) {
    const nextStatus = getMatchStatus(match.startTime, match.endTime);
    if (!nextStatus) {
        return match.status;
    }
    if (match.status !== nextStatus) {
        await updateStatus(nextStatus);
        match.status = nextStatus;
    }
    return match.status;
}