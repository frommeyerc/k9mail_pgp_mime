
package com.fsck.k9.mail;

public abstract class BodyPart implements Part {
    private Multipart mParent;

    //TODO will be superfluous as soon as Part has a getMessage operation
    public Multipart getParent() {
        return mParent;
    }

    public void setParent(Multipart parent) {
        mParent = parent;
    }

    public abstract void setEncoding(String encoding) throws MessagingException;

    public abstract void setUsing7bitTransport() throws MessagingException;
}
