package com.fsck.k9.mail.store.local;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import com.fsck.k9.helper.Utility;
import com.fsck.k9.mail.Flag;

abstract class LocalStoreUtil {

	private LocalStoreUtil() {
	}

    /**
     * Flattens the array of {@link Flag}s into a {@link String} while omitting
     * the otherwise stored {@link Flag}s:
     * <ul>
     * <li> {@link Flag#DELETED}</li>
     * <li> {@link Flag#SEEN}</li>
     * <li> {@link Flag#FLAGGED}</li>
     * <li> {@link Flag#ANSWERED}</li>
     * <li> {@link Flag#FORWARDED}</li>
     * </ul>
     * @param flags the flags to convert
     * @return a String containing a comma separated list of flags
     */
    public static String serializeFlags(Flag[] flags) {
        List<Flag> extraFlags = new ArrayList<Flag>();

        for (Flag flag : flags) {
            switch (flag) {
                case DELETED:
                case SEEN:
                case FLAGGED:
                case ANSWERED:
                case FORWARDED: {
                    break;
                }
                default: {
                    extraFlags.add(flag);
                }
            }
        }

        return Utility.combine(extraFlags.toArray(new Flag[extraFlags.size()]), ',').toUpperCase(Locale.US);
    }

}
