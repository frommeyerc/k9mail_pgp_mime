package com.fsck.k9.mail.store.local;

import static com.fsck.k9.mail.store.local.QueryBuilder.ATTACHMENTS;
import static com.fsck.k9.mail.store.local.QueryBuilder.ATTACH_DATA;
import static com.fsck.k9.mail.store.local.QueryBuilder.CONTENT_DATA;
import static com.fsck.k9.mail.store.local.QueryBuilder.DATE;
import static com.fsck.k9.mail.store.local.QueryBuilder.DISPLAY_CLASS;
import static com.fsck.k9.mail.store.local.QueryBuilder.FOLDERS;
import static com.fsck.k9.mail.store.local.QueryBuilder.FOLDER_COLS;
import static com.fsck.k9.mail.store.local.QueryBuilder.FOLDER_ID;
import static com.fsck.k9.mail.store.local.QueryBuilder.HEADERS;
import static com.fsck.k9.mail.store.local.QueryBuilder.HEADER_DATA;
import static com.fsck.k9.mail.store.local.QueryBuilder.ID;
import static com.fsck.k9.mail.store.local.QueryBuilder.INTEGRATE;
import static com.fsck.k9.mail.store.local.QueryBuilder.LAST_PUSH;
import static com.fsck.k9.mail.store.local.QueryBuilder.LAST_UPDATE;
import static com.fsck.k9.mail.store.local.QueryBuilder.MESSAGES;
import static com.fsck.k9.mail.store.local.QueryBuilder.MESSAGES_COLS;
import static com.fsck.k9.mail.store.local.QueryBuilder.MESSAGE_ID;
import static com.fsck.k9.mail.store.local.QueryBuilder.NAME;
import static com.fsck.k9.mail.store.local.QueryBuilder.POLL_CLASS;
import static com.fsck.k9.mail.store.local.QueryBuilder.PUSH_CLASS;
import static com.fsck.k9.mail.store.local.QueryBuilder.PUSH_STATE;
import static com.fsck.k9.mail.store.local.QueryBuilder.STATUS;
import static com.fsck.k9.mail.store.local.QueryBuilder.THREADS;
import static com.fsck.k9.mail.store.local.QueryBuilder.THREAD_COLS;
import static com.fsck.k9.mail.store.local.QueryBuilder.TOP_GROUP;
import static com.fsck.k9.mail.store.local.QueryBuilder.VISIBLE_LIMIT;
import static com.fsck.k9.mail.store.local.QueryBuilder.insert;
import static com.fsck.k9.mail.store.local.QueryBuilder.select;
import static com.fsck.k9.mail.store.local.QueryBuilder.update;
import static com.fsck.k9.mail.store.local.QueryBuilder.DataBuilder.map;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.james.mime4j.util.MimeUtil;

import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.util.Log;

import com.fsck.k9.Account;
import com.fsck.k9.Account.MessageFormat;
import com.fsck.k9.K9;
import com.fsck.k9.activity.Search;
import com.fsck.k9.controller.MessageRemovalListener;
import com.fsck.k9.controller.MessageRetrievalListener;
import com.fsck.k9.helper.HtmlConverter;
import com.fsck.k9.helper.Utility;
import com.fsck.k9.mail.Body;
import com.fsck.k9.mail.BodyPart;
import com.fsck.k9.mail.FetchProfile;
import com.fsck.k9.mail.Flag;
import com.fsck.k9.mail.Folder;
import com.fsck.k9.mail.Message;
import com.fsck.k9.mail.Message.RecipientType;
import com.fsck.k9.mail.MessagingException;
import com.fsck.k9.mail.Part;
import com.fsck.k9.mail.internet.MimeBodyPart;
import com.fsck.k9.mail.internet.MimeHeader;
import com.fsck.k9.mail.internet.MimeMessage;
import com.fsck.k9.mail.internet.MimeMultipart;
import com.fsck.k9.mail.internet.MimeUtility;
import com.fsck.k9.mail.internet.MimeUtility.ViewableContainer;
import com.fsck.k9.mail.internet.TextBody;
import com.fsck.k9.mail.store.LockableDatabase.DbCallback;
import com.fsck.k9.mail.store.LockableDatabase.WrappedException;
import com.fsck.k9.mail.store.StorageManager;
import com.fsck.k9.mail.store.UnavailableStorageException;
import com.fsck.k9.mail.store.local.LocalStore.ThreadInfo;
import com.fsck.k9.mail.store.local.QueryBuilder.DataBuilder;
import com.fsck.k9.mail.store.local.QueryBuilder.JoinBuilder;
import com.fsck.k9.mail.store.local.QueryBuilder.SelectBuilder;
import com.fsck.k9.mail.store.local.QueryBuilder.WhereBuilder;
import com.fsck.k9.provider.AttachmentProvider;

public class LocalFolder extends Folder implements Serializable {

	private static final long serialVersionUID = -1973296520918624767L;
	
	private final LocalStore localStore;
	
    private String mName = null;
    private long mFolderId = -1;
    private int mVisibleLimit = -1;
    private String prefId = null;
    private FolderClass mDisplayClass = FolderClass.NO_CLASS;
    private FolderClass mSyncClass = FolderClass.INHERITED;
    private FolderClass mPushClass = FolderClass.SECOND_CLASS;
    private boolean mInTopGroup = false;
    private String mPushState = null;
    private boolean mIntegrate = false;
    // mLastUid is used during syncs. It holds the highest UID within the local folder so we
    // know whether or not an unread message added to the local folder is actually "new" or not.
    private Integer mLastUid = null;

    public LocalFolder(LocalStore localStore, Account account, String name) {
        super(account);
		this.localStore = localStore;
        this.mName = name;

        if (account.getInboxFolderName().equals(getName())) {
            mSyncClass =  FolderClass.FIRST_CLASS;
            mPushClass =  FolderClass.FIRST_CLASS;
            mInTopGroup = true;
        }
    }

    public LocalFolder(LocalStore localStore, Account account, long id) {
        super(account);
		this.localStore = localStore;
        this.mFolderId = id;
    }

    public long getId() {
        return mFolderId;
    }

    @Override
    public void open(final int mode) throws MessagingException {
        if (isOpen() && (getMode() == mode || mode == OPEN_MODE_RO)) {
            return;
        } else if (isOpen()) {
            //previously opened in READ_ONLY and now requesting READ_WRITE
            //so close connection and reopen
            close();
        }

        try {
            this.localStore.database.execute(false, new DbCallback<Void>() {
                @Override
                public Void doDbWork(final SQLiteDatabase db) throws WrappedException {
                    Cursor cursor = null;
                    try {
                    	cursor = select(FOLDERS).cols(FOLDER_COLS).where(WhereBuilder.cond().folder(mName, mFolderId)).execute(db);
                        if (cursor.moveToFirst() && !cursor.isNull(LocalStore.FOLDER_ID_INDEX)) {
                            int folderId = cursor.getInt(LocalStore.FOLDER_ID_INDEX);
                            if (folderId > 0) {
                                open(cursor);
                            }
                        } else {
                            Log.w(K9.LOG_TAG, "Creating folder " + getName() + " with existing id " + getId());
                            create(FolderType.HOLDS_MESSAGES);
                            open(mode);
                        }
                    } catch (MessagingException e) {
                        throw new WrappedException(e);
                    } finally {
                        Utility.closeQuietly(cursor);
                    }
                    return null;
                }
            });
        } catch (WrappedException e) {
            throw(MessagingException) e.getCause();
        }
    }

    void open(Cursor cursor) throws MessagingException {
        mFolderId = cursor.getInt(LocalStore.FOLDER_ID_INDEX);
        mName = cursor.getString(LocalStore.FOLDER_NAME_INDEX);
        mVisibleLimit = cursor.getInt(LocalStore.FOLDER_VISIBLE_LIMIT_INDEX);
        mPushState = cursor.getString(LocalStore.FOLDER_PUSH_STATE_INDEX);
        super.setStatus(cursor.getString(LocalStore.FOLDER_STATUS_INDEX));
        // Only want to set the local variable stored in the super class.  This class
        // does a DB update on setLastChecked
        super.setLastChecked(cursor.getLong(LocalStore.FOLDER_LAST_CHECKED_INDEX));
        super.setLastPush(cursor.getLong(LocalStore.FOLDER_LAST_PUSHED_INDEX));
        mInTopGroup = (cursor.getInt(LocalStore.FOLDER_TOP_GROUP_INDEX)) == 1  ? true : false;
        mIntegrate = (cursor.getInt(LocalStore.FOLDER_INTEGRATE_INDEX) == 1) ? true : false;
        String noClass = FolderClass.NO_CLASS.toString();
        String displayClass = cursor.getString(LocalStore.FOLDER_DISPLAY_CLASS_INDEX);
        mDisplayClass = Folder.FolderClass.valueOf((displayClass == null) ? noClass : displayClass);
        String pushClass = cursor.getString(LocalStore.FOLDER_PUSH_CLASS_INDEX);
        mPushClass = Folder.FolderClass.valueOf((pushClass == null) ? noClass : pushClass);
        String syncClass = cursor.getString(LocalStore.FOLDER_SYNC_CLASS_INDEX);
        mSyncClass = Folder.FolderClass.valueOf((syncClass == null) ? noClass : syncClass);
    }

    @Override
    public boolean isOpen() {
        return (mFolderId != -1 && mName != null);
    }

    @Override
    public int getMode() {
        return OPEN_MODE_RW;
    }

    @Override
    public String getName() {
        return mName;
    }

    @Override
    public boolean exists() throws MessagingException {
        return this.localStore.database.execute(false, new DbCallback<Boolean>() {
            @Override
            public Boolean doDbWork(final SQLiteDatabase db) throws WrappedException {
                Cursor cursor = null;
                try {
                	cursor = select(FOLDERS).cols("id").where(WhereBuilder.cond().folder(mName)).execute(db);
                    if (cursor.moveToFirst()) {
                        int folderId = cursor.getInt(0);
                        return (folderId > 0);
                    }

                    return false;
                } finally {
                    Utility.closeQuietly(cursor);
                }
            }
        });
    }

    @Override
    public boolean create(FolderType type) throws MessagingException {
        return create(type, mAccount.getDisplayCount());
    }

    @Override
    public boolean create(FolderType type, final int visibleLimit) throws MessagingException {
        if (exists()) {
            throw new MessagingException("Folder " + mName + " already exists.");
        }
        List<LocalFolder> foldersToCreate = new ArrayList<LocalFolder>(1);
        foldersToCreate.add(this);
        this.localStore.createFolders(foldersToCreate, visibleLimit);

        return true;
    }

    class PreferencesHolder {
        FolderClass displayClass = mDisplayClass;
        FolderClass syncClass = mSyncClass;
        FolderClass pushClass = mPushClass;
        boolean inTopGroup = mInTopGroup;
        boolean integrate = mIntegrate;
    }

    @Override
    public void close() {
        mFolderId = -1;
    }

    @Override
    public int getMessageCount() throws MessagingException {
        try {
            return this.localStore.database.execute(false, new DbCallback<Integer>() {
                @Override
                public Integer doDbWork(final SQLiteDatabase db) throws WrappedException {
                    try {
                        open(OPEN_MODE_RW);
                    } catch (MessagingException e) {
                        throw new WrappedException(e);
                    }
                    Cursor cursor = null;
                    try {
                    	cursor = select(MESSAGES).cols("COUNT(id)").where(
                    			WhereBuilder.cond().notEmpty().and().notDeleted().and().folderId(mFolderId)).execute(db);
                        cursor.moveToFirst();
                        return cursor.getInt(0);   //messagecount
                    } finally {
                        Utility.closeQuietly(cursor);
                    }
                }
            });
        } catch (WrappedException e) {
            throw(MessagingException) e.getCause();
        }
    }

    @Override
    public int getUnreadMessageCount() throws MessagingException {
        if (mFolderId == -1) {
            open(OPEN_MODE_RW);
        }

        try {
            return this.localStore.database.execute(false, new DbCallback<Integer>() {
                @Override
                public Integer doDbWork(final SQLiteDatabase db) throws WrappedException {
                    int unreadMessageCount = 0;
                    Cursor cursor = select(MESSAGES).cols("COUNT(id)").where(WhereBuilder.cond().notEmpty().and().notDeleted()
                    		.and().notRead().and().folderId(mFolderId)).execute(db);
                    try {
                        if (cursor.moveToFirst()) {
                            unreadMessageCount = cursor.getInt(0);
                        }
                    } finally {
                        cursor.close();
                    }

                    return unreadMessageCount;
                }
            });
        } catch (WrappedException e) {
            throw(MessagingException) e.getCause();
        }
    }

    @Override
    public int getFlaggedMessageCount() throws MessagingException {
        if (mFolderId == -1) {
            open(OPEN_MODE_RW);
        }

        try {
            return this.localStore.database.execute(false, new DbCallback<Integer>() {
                @Override
                public Integer doDbWork(final SQLiteDatabase db) throws WrappedException {
                    int flaggedMessageCount = 0;
                    Cursor cursor = select(MESSAGES).cols("COUNT(id)").where(WhereBuilder.cond().notEmpty().and().notDeleted()
                    		.and().flagged().and().folderId(mFolderId)).execute(db);
                    try {
                        if (cursor.moveToFirst()) {
                            flaggedMessageCount = cursor.getInt(0);
                        }
                    } finally {
                        cursor.close();
                    }

                    return flaggedMessageCount;
                }
            });
        } catch (WrappedException e) {
            throw(MessagingException) e.getCause();
        }
    }

    @Override
    public void setLastChecked(final long lastChecked) throws MessagingException {
        try {
            open(OPEN_MODE_RW);
            LocalFolder.super.setLastChecked(lastChecked);
        } catch (MessagingException e) {
            throw new WrappedException(e);
        }
        updateFolderColumn(LAST_UPDATE, lastChecked);
    }

    @Override
    public void setLastPush(final long lastChecked) throws MessagingException {
        try {
            open(OPEN_MODE_RW);
            LocalFolder.super.setLastPush(lastChecked);
        } catch (MessagingException e) {
            throw new WrappedException(e);
        }
        updateFolderColumn(LAST_PUSH, lastChecked);
    }

    public int getVisibleLimit() throws MessagingException {
        open(OPEN_MODE_RW);
        return mVisibleLimit;
    }

    public void purgeToVisibleLimit(MessageRemovalListener listener) throws MessagingException {
        //don't purge messages while a Search is active since it might throw away search results
        if (!Search.isActive()) {
            if (mVisibleLimit == 0) {
                return ;
            }
            open(OPEN_MODE_RW);
            Message[] messages = getMessages(null, false);
            for (int i = mVisibleLimit; i < messages.length; i++) {
                if (listener != null) {
                    listener.messageRemoved(messages[i]);
                }
                messages[i].destroy();
            }
        }
    }


    public void setVisibleLimit(final int visibleLimit) throws MessagingException {
        mVisibleLimit = visibleLimit;
        updateFolderColumn(VISIBLE_LIMIT, mVisibleLimit);
    }

    @Override
    public void setStatus(final String status) throws MessagingException {
        updateFolderColumn(STATUS, status);
    }
    public void setPushState(final String pushState) throws MessagingException {
        mPushState = pushState;
        updateFolderColumn(PUSH_STATE, pushState);
    }

    private void updateFolderColumn(final String column, final Object value) throws MessagingException {
        try {
            this.localStore.database.execute(false, new DbCallback<Void>() {
                @Override
                public Void doDbWork(final SQLiteDatabase db) throws WrappedException {
                    try {
                        open(OPEN_MODE_RW);
                    } catch (MessagingException e) {
                        throw new WrappedException(e);
                    }
                    update(FOLDERS).data(DataBuilder.map().put(column, value.toString()).build()).where(WhereBuilder.cond().id(mFolderId)).execute(db);
                    return null;
                }
            });
        } catch (WrappedException e) {
            throw(MessagingException) e.getCause();
        }
    }

    public String getPushState() {
        return mPushState;
    }
    @Override
    public FolderClass getDisplayClass() {
        return mDisplayClass;
    }

    @Override
    public FolderClass getSyncClass() {
        return (FolderClass.INHERITED == mSyncClass) ? getDisplayClass() : mSyncClass;
    }

    public FolderClass getRawSyncClass() {
        return mSyncClass;
    }

    @Override
    public FolderClass getPushClass() {
        return (FolderClass.INHERITED == mPushClass) ? getSyncClass() : mPushClass;
    }

    public FolderClass getRawPushClass() {
        return mPushClass;
    }

    public void setDisplayClass(FolderClass displayClass) throws MessagingException {
        mDisplayClass = displayClass;
        updateFolderColumn(DISPLAY_CLASS, mDisplayClass.name());

    }

    public void setSyncClass(FolderClass syncClass) throws MessagingException {
        mSyncClass = syncClass;
        updateFolderColumn(POLL_CLASS, mSyncClass.name());
    }
    
    public void setPushClass(FolderClass pushClass) throws MessagingException {
        mPushClass = pushClass;
        updateFolderColumn(PUSH_CLASS, mPushClass.name());
    }

    public boolean isIntegrate() {
        return mIntegrate;
    }
    
    public void setIntegrate(boolean integrate) throws MessagingException {
        mIntegrate = integrate;
        updateFolderColumn(INTEGRATE, mIntegrate ? 1 : 0);
    }

    private String getPrefId(String name) {
        if (prefId == null) {
            prefId = this.localStore.uUid + "." + name;
        }

        return prefId;
    }

    private String getPrefId() throws MessagingException {
        open(OPEN_MODE_RW);
        return getPrefId(mName);

    }

    public void delete() throws MessagingException {
        String id = getPrefId();

        SharedPreferences.Editor editor = this.localStore.getPreferences().edit();

        editor.remove(id + ".displayMode");
        editor.remove(id + ".syncMode");
        editor.remove(id + ".pushMode");
        editor.remove(id + ".inTopGroup");
        editor.remove(id + ".integrate");

        editor.commit();
    }

    public void save() throws MessagingException {
        SharedPreferences.Editor editor = this.localStore.getPreferences().edit();
        save(editor);
        editor.commit();
    }

    public void save(SharedPreferences.Editor editor) throws MessagingException {
        String id = getPrefId();

        // there can be a lot of folders.  For the defaults, let's not save prefs, saving space, except for INBOX
        if (mDisplayClass == FolderClass.NO_CLASS && !mAccount.getInboxFolderName().equals(getName())) {
            editor.remove(id + ".displayMode");
        } else {
            editor.putString(id + ".displayMode", mDisplayClass.name());
        }

        if (mSyncClass == FolderClass.INHERITED && !mAccount.getInboxFolderName().equals(getName())) {
            editor.remove(id + ".syncMode");
        } else {
            editor.putString(id + ".syncMode", mSyncClass.name());
        }

        if (mPushClass == FolderClass.SECOND_CLASS && !mAccount.getInboxFolderName().equals(getName())) {
            editor.remove(id + ".pushMode");
        } else {
            editor.putString(id + ".pushMode", mPushClass.name());
        }
        editor.putBoolean(id + ".inTopGroup", mInTopGroup);

        editor.putBoolean(id + ".integrate", mIntegrate);

    }

    public void refresh(String name, PreferencesHolder prefHolder) {
        String id = getPrefId(name);

        SharedPreferences preferences = this.localStore.getPreferences();

        try {
            prefHolder.displayClass = FolderClass.valueOf(preferences.getString(id + ".displayMode",
                                      prefHolder.displayClass.name()));
        } catch (Exception e) {
            Log.e(K9.LOG_TAG, "Unable to load displayMode for " + getName(), e);
        }
        if (prefHolder.displayClass == FolderClass.NONE) {
            prefHolder.displayClass = FolderClass.NO_CLASS;
        }

        try {
            prefHolder.syncClass = FolderClass.valueOf(preferences.getString(id  + ".syncMode",
                                   prefHolder.syncClass.name()));
        } catch (Exception e) {
            Log.e(K9.LOG_TAG, "Unable to load syncMode for " + getName(), e);

        }
        if (prefHolder.syncClass == FolderClass.NONE) {
            prefHolder.syncClass = FolderClass.INHERITED;
        }

        try {
            prefHolder.pushClass = FolderClass.valueOf(preferences.getString(id  + ".pushMode",
                                   prefHolder.pushClass.name()));
        } catch (Exception e) {
            Log.e(K9.LOG_TAG, "Unable to load pushMode for " + getName(), e);
        }
        if (prefHolder.pushClass == FolderClass.NONE) {
            prefHolder.pushClass = FolderClass.INHERITED;
        }
        prefHolder.inTopGroup = preferences.getBoolean(id + ".inTopGroup", prefHolder.inTopGroup);
        prefHolder.integrate = preferences.getBoolean(id + ".integrate", prefHolder.integrate);

    }

    @Override
    public void fetch(final Message[] messages, final FetchProfile fp, final MessageRetrievalListener listener)
    throws MessagingException {
        try {
            this.localStore.database.execute(false, new DbCallback<Void>() {
                @Override
                public Void doDbWork(final SQLiteDatabase db) throws WrappedException {
                    try {
                        open(OPEN_MODE_RW);
                        if (fp.contains(FetchProfile.Item.BODY)) {
                            for (Message message : messages) {
                                LocalMessage localMessage = (LocalMessage)message;
                                Cursor cursor = null;
                                MimeMultipart mp = new MimeMultipart();
                                mp.setSubType("mixed");
                                try {
                                	cursor = select(MESSAGES).cols(CONTENT_DATA).where(WhereBuilder.cond().id(localMessage.mId)).execute(db);
                                    cursor.moveToNext();
                                    String htmlContent = cursor.getString(0);
                                    String textContent = cursor.getString(1);
                                    String mimeType = cursor.getString(2);
                                    
                                    
                                    // FIXME: Extract from here!!
                                    if (mimeType != null && mimeType.toLowerCase(Locale.US).startsWith("multipart/")) {
                                        // If this is a multipart message, preserve both text
                                        // and html parts, as well as the subtype.
                                        mp.setSubType(mimeType.toLowerCase(Locale.US).replaceFirst("^multipart/", ""));
                                        if (textContent != null) {
                                            LocalTextBody body = new LocalTextBody(textContent, htmlContent);
                                            MimeBodyPart bp = new MimeBodyPart(body, "text/plain");
                                            mp.addBodyPart(bp);
                                        }

                                        if (mAccount.getMessageFormat() != MessageFormat.TEXT) {
                                            if (htmlContent != null) {
                                                TextBody body = new TextBody(htmlContent);
                                                MimeBodyPart bp = new MimeBodyPart(body, "text/html");
                                                mp.addBodyPart(bp);
                                            }

                                            // If we have both text and html content and our MIME type
                                            // isn't multipart/alternative, then corral them into a new
                                            // multipart/alternative part and put that into the parent.
                                            // If it turns out that this is the only part in the parent
                                            // MimeMultipart, it'll get fixed below before we attach to
                                            // the message.
                                            if (textContent != null && htmlContent != null && !mimeType.equalsIgnoreCase("multipart/alternative")) {
                                                MimeMultipart alternativeParts = mp;
                                                alternativeParts.setSubType("alternative");
                                                mp = new MimeMultipart();
                                                mp.addBodyPart(new MimeBodyPart(alternativeParts));
                                            }
                                        }
                                    } else if (mimeType != null && mimeType.equalsIgnoreCase("text/plain")) {
                                        // If it's text, add only the plain part. The MIME
                                        // container will drop away below.
                                        if (textContent != null) {
                                            LocalTextBody body = new LocalTextBody(textContent, htmlContent);
                                            MimeBodyPart bp = new MimeBodyPart(body, "text/plain");
                                            mp.addBodyPart(bp);
                                        }
                                    } else if (mimeType != null && mimeType.equalsIgnoreCase("text/html")) {
                                        // If it's html, add only the html part. The MIME
                                        // container will drop away below.
                                        if (htmlContent != null) {
                                            TextBody body = new TextBody(htmlContent);
                                            MimeBodyPart bp = new MimeBodyPart(body, "text/html");
                                            mp.addBodyPart(bp);
                                        }
                                    } else {
                                        // MIME type not set. Grab whatever part we can get,
                                        // with Text taking precedence. This preserves pre-HTML
                                        // composition behaviour.
                                        if (textContent != null) {
                                            LocalTextBody body = new LocalTextBody(textContent, htmlContent);
                                            MimeBodyPart bp = new MimeBodyPart(body, "text/plain");
                                            mp.addBodyPart(bp);
                                        } else if (htmlContent != null) {
                                            TextBody body = new TextBody(htmlContent);
                                            MimeBodyPart bp = new MimeBodyPart(body, "text/html");
                                            mp.addBodyPart(bp);
                                        }
                                    }

                                } catch (Exception e) {
                                    Log.e(K9.LOG_TAG, "Exception fetching message:", e);
                                } finally {
                                    Utility.closeQuietly(cursor);
                                }

                                try {
                                	cursor = select(ATTACHMENTS).cols(ATTACH_DATA).where(WhereBuilder.cond().messageId(localMessage.mId)).execute(db);
                                    while (cursor.moveToNext()) {
                                        long id = cursor.getLong(0);
                                        int size = cursor.getInt(1);
                                        String name = cursor.getString(2);
                                        String type = cursor.getString(3);
                                        String storeData = cursor.getString(4);
                                        String contentUri = cursor.getString(5);
                                        String contentId = cursor.getString(6);
                                        String contentDisposition = cursor.getString(7);
                                        String encoding = MimeUtility.getEncodingforType(type);
                                        Body body = null;

                                        if (contentDisposition == null) {
                                            contentDisposition = "attachment";
                                        }

                                        if (contentUri != null) {
                                            if (MimeUtil.isMessage(type)) {
                                                body = new LocalAttachmentMessageBody(
                                                        Uri.parse(contentUri),
                                                        LocalFolder.this.localStore.mApplication);
                                            } else {
                                                body = new LocalAttachmentBody(
                                                        Uri.parse(contentUri),
                                                        LocalFolder.this.localStore.mApplication);
                                            }
                                        }

                                        MimeBodyPart bp = new LocalAttachmentBodyPart(body, id);
                                        bp.setEncoding(encoding);
                                        if (name != null) {
                                            bp.setHeader(MimeHeader.HEADER_CONTENT_TYPE,
                                                         String.format("%s;\r\n name=\"%s\"",
                                                                       type,
                                                                       name));
                                            bp.setHeader(MimeHeader.HEADER_CONTENT_DISPOSITION,
                                                         String.format(Locale.US, "%s;\r\n filename=\"%s\";\r\n size=%d",
                                                                       contentDisposition,
                                                                       name, // TODO: Should use encoded word defined in RFC 2231.
                                                                       size));
                                        } else {
                                            bp.setHeader(MimeHeader.HEADER_CONTENT_TYPE, type);
                                            bp.setHeader(MimeHeader.HEADER_CONTENT_DISPOSITION,
                                                    String.format(Locale.US, "%s;\r\n size=%d",
                                                                  contentDisposition,
                                                                  size));
                                        }

                                        bp.setHeader(MimeHeader.HEADER_CONTENT_ID, contentId);
                                        /*
                                         * HEADER_ANDROID_ATTACHMENT_STORE_DATA is a custom header we add to that
                                         * we can later pull the attachment from the remote store if necessary.
                                         */
                                        bp.setHeader(MimeHeader.HEADER_ANDROID_ATTACHMENT_STORE_DATA, storeData);

                                        mp.addBodyPart(bp);
                                    }
                                } finally {
                                    Utility.closeQuietly(cursor);
                                }

                                if (mp.getCount() == 0) {
                                    // If we have no body, remove the container and create a
                                    // dummy plain text body. This check helps prevents us from
                                    // triggering T_MIME_NO_TEXT and T_TVD_MIME_NO_HEADERS
                                    // SpamAssassin rules.
                                    localMessage.setHeader(MimeHeader.HEADER_CONTENT_TYPE, "text/plain");
                                    localMessage.setBody(new TextBody(""));
                                } else if (mp.getCount() == 1 && (mp.getBodyPart(0) instanceof LocalAttachmentBodyPart) == false)

                                {
                                    // If we have only one part, drop the MimeMultipart container.
                                    BodyPart part = mp.getBodyPart(0);
                                    localMessage.setHeader(MimeHeader.HEADER_CONTENT_TYPE, part.getContentType());
                                    localMessage.setBody(part.getBody());
                                } else {
                                    // Otherwise, attach the MimeMultipart to the message.
                                    localMessage.setBody(mp);
                                }
                            }
                        }
                    } catch (MessagingException e) {
                        throw new WrappedException(e);
                    }
                    return null;
                }
            });
        } catch (WrappedException e) {
            throw(MessagingException) e.getCause();
        }
    }

    @Override
    public Message[] getMessages(int start, int end, Date earliestDate, MessageRetrievalListener listener)
    throws MessagingException {
        open(OPEN_MODE_RW);
        throw new MessagingException(
            "LocalStore.getMessages(int, int, MessageRetrievalListener) not yet implemented");
    }

    /**
     * Populate the header fields of the given list of messages by reading
     * the saved header data from the database.
     *
     * @param messages
     *            The messages whose headers should be loaded.
     * @throws UnavailableStorageException
     */
    void populateHeaders(final List<LocalMessage> messages) throws UnavailableStorageException {
        this.localStore.database.execute(false, new DbCallback<Void>() {
            @Override
            public Void doDbWork(final SQLiteDatabase db) throws WrappedException, UnavailableStorageException {
                Cursor cursor = null;
                if (messages.isEmpty()) {
                    return null;
                }
                try {
                    Map<Long, LocalMessage> popMessages = new HashMap<Long, LocalMessage>();
                    for (LocalMessage message : messages) {
                        popMessages.put(message.getId(), message);

                    }
                    cursor = select(HEADERS).cols(HEADER_DATA).where(WhereBuilder.cond().inMessages(popMessages.keySet())).orderBy(ID, true).execute(db);

                    while (cursor.moveToNext()) {
                        Long id = cursor.getLong(0);
                        String name = cursor.getString(1);
                        String value = cursor.getString(2);
                        popMessages.get(id).addHeader(name, value);
                    }
                } finally {
                    Utility.closeQuietly(cursor);
                }
                return null;
            }
        });
    }

    public String getMessageUidById(final long id) throws MessagingException {
        try {
            return this.localStore.database.execute(false, new DbCallback<String>() {
                @Override
                public String doDbWork(final SQLiteDatabase db) throws WrappedException, UnavailableStorageException {
                    try {
                        open(OPEN_MODE_RW);
                        Cursor cursor = null;

                        try {
                        	cursor = select(MESSAGES).cols("uid").where(WhereBuilder.cond().id(id).and().folderId(mFolderId)).execute(db);
                            if (!cursor.moveToNext()) {
                                return null;
                            }
                            return cursor.getString(0);
                        } finally {
                            Utility.closeQuietly(cursor);
                        }
                    } catch (MessagingException e) {
                        throw new WrappedException(e);
                    }
                }
            });
        } catch (WrappedException e) {
            throw(MessagingException) e.getCause();
        }
    }

    @Override
    public LocalMessage getMessage(final String uid) throws MessagingException {
        try {
            return this.localStore.database.execute(false, new DbCallback<LocalMessage>() {
                @Override
                public LocalMessage doDbWork(final SQLiteDatabase db) throws WrappedException, UnavailableStorageException {
                    try {
                        open(OPEN_MODE_RW);
                        LocalMessage message = new LocalMessage(LocalFolder.this.localStore, LocalFolder.this.mAccount, uid, LocalFolder.this);
                        Cursor cursor = null;

                        try {
                        	cursor = select(new JoinBuilder(MESSAGES).lJoin(THREADS).on(THREADS, MESSAGE_ID, MESSAGES, ID)).cols(MESSAGES_COLS)
                        			.where(WhereBuilder.cond().uId(message.getUid()).and().folderId(mFolderId)).execute(db);
                            if (!cursor.moveToNext()) {
                                return null;
                            }
                            message.populateFromGetMessageCursor(cursor);
                        } finally {
                            Utility.closeQuietly(cursor);
                        }
                        return message;
                    } catch (MessagingException e) {
                        throw new WrappedException(e);
                    }
                }
            });
        } catch (WrappedException e) {
            throw(MessagingException) e.getCause();
        }
    }

    @Override
    public Message[] getMessages(MessageRetrievalListener listener) throws MessagingException {
        return getMessages(listener, true);
    }

    @Override
    public Message[] getMessages(final MessageRetrievalListener listener, final boolean includeDeleted) throws MessagingException {
        try {
            return this.localStore.database.execute(false, new DbCallback<Message[]>() {
                @Override
                public Message[] doDbWork(final SQLiteDatabase db) throws WrappedException, UnavailableStorageException {
                    try {
                        open(OPEN_MODE_RW);
                        WhereBuilder wb = WhereBuilder.cond().folderId(mFolderId).and().notEmpty();
                        if (!includeDeleted)
                        	wb.and().notDeleted();
                        SelectBuilder query = select(new JoinBuilder(MESSAGES).lJoin(THREADS).on(MESSAGES, ID, THREADS, MESSAGE_ID))
                        		.where(wb).orderBy(DATE, false);
                        return LocalFolder.this.localStore.getMessages(listener, LocalFolder.this, query);
                    } catch (MessagingException e) {
                        throw new WrappedException(e);
                    }
                }
            });
        } catch (WrappedException e) {
            throw(MessagingException) e.getCause();
        }
    }

    @Override
    public Message[] getMessages(String[] uids, MessageRetrievalListener listener)
    throws MessagingException {
        open(OPEN_MODE_RW);
        if (uids == null) {
            return getMessages(listener);
        }
        ArrayList<Message> messages = new ArrayList<Message>();
        for (String uid : uids) {
            Message message = getMessage(uid);
            if (message != null) {
                messages.add(message);
            }
        }
        return messages.toArray(LocalStore.EMPTY_MESSAGE_ARRAY);
    }

    @Override
    public Map<String, String> copyMessages(Message[] msgs, Folder folder) throws MessagingException {
        if (!(folder instanceof LocalFolder)) {
            throw new MessagingException("copyMessages called with incorrect Folder");
        }
        return ((LocalFolder) folder).appendMessages(msgs, true);
    }

    @Override
    public Map<String, String> moveMessages(final Message[] msgs, final Folder destFolder) throws MessagingException {
        if (!(destFolder instanceof LocalFolder)) {
            throw new MessagingException("moveMessages called with non-LocalFolder");
        }

        final LocalFolder lDestFolder = (LocalFolder)destFolder;

        final Map<String, String> uidMap = new HashMap<String, String>();

        try {
            this.localStore.database.execute(false, new DbCallback<Void>() {
                @Override
                public Void doDbWork(final SQLiteDatabase db) throws WrappedException, UnavailableStorageException {
                    try {
                        lDestFolder.open(OPEN_MODE_RW);
                        for (Message message : msgs) {
                            if (K9.DEBUG) {
                                Log.d(K9.LOG_TAG, "Updating folder_id to " + lDestFolder.getId() + " for message with UID "
                                      + message.getUid() + ", id " + message.getId() + " currently in folder " + getName());
                            }

                            String newUid = K9.LOCAL_UID_PREFIX + UUID.randomUUID().toString();
                            String oldUID = message.getUid();
                            message.setUid(newUid);

                            uidMap.put(oldUID, newUid);

                            // "Move" the message into the new folder
                            long msgId = message.getId();
                            update(MESSAGES).data(DataBuilder.map().put(FOLDER_ID, lDestFolder.getId()).put("uid", newUid).build())
                            		.where(WhereBuilder.cond().id(msgId)).execute(db);

                            // Create/update entry in 'threads' table for the message in the target folder
                            ThreadInfo threadInfo = lDestFolder.doMessageThreading(db, message);
                            DataBuilder dataBuilder = DataBuilder.map().put(MESSAGE_ID, msgId);
                            if (threadInfo.threadId == -1) {
                            	// Message threading in the target folder
                            	dataBuilder.putU("root", threadInfo.rootId, -1).putU("parent", threadInfo.parentId, -1);
								insert(THREADS).data(dataBuilder.build()).execute(db);
                            } else {
                            	update(THREADS).data(dataBuilder.build()).where(WhereBuilder.cond().id(threadInfo.threadId)).execute(db);
                            }

                            /*
                             * Add a placeholder message so we won't download the original
                             * message again if we synchronize before the remote move is
                             * complete.
                             */

                            // We need to open this folder to get the folder id
                            open(OPEN_MODE_RW);

                            DataBuilder cv = DataBuilder.map().put("uid", oldUID).putNull("flags").put("read", 1).put("deleted", 1).put("folder_id", mFolderId);
                            cv.put("empty", 0).putNotNull(MESSAGE_ID, message.getMessageId());

                            final long newId;
                            if (threadInfo.msgId != -1) {
                                // There already existed an empty message in the target folder.
                                // Let's use it as placeholder.
                                newId = threadInfo.msgId;
                                update(MESSAGES).data(cv.build()).where(WhereBuilder.cond().id(newId)).execute(db);
                            } else {
                                newId = insert(MESSAGES).data(cv.build()).execute(db);
                            }

                            /*
                             * Update old entry in 'threads' table to point to the newly
                             * created placeholder.
                             */
                            update(THREADS).data(DataBuilder.map().put(MESSAGE_ID, newId).build())
                            		.where(WhereBuilder.cond().id(((LocalMessage) message).getThreadId())).execute(db);
                        }
                    } catch (MessagingException e) {
                        throw new WrappedException(e);
                    }
                    return null;
                }
            });

            this.localStore.notifyChange();

            return uidMap;
        } catch (WrappedException e) {
            throw(MessagingException) e.getCause();
        }

    }

    /**
     * Convenience transaction wrapper for storing a message and set it as fully downloaded. Implemented mainly to speed up DB transaction commit.
     *
     * @param message Message to store. Never <code>null</code>.
     * @param runnable What to do before setting {@link Flag#X_DOWNLOADED_FULL}. Never <code>null</code>.
     * @return The local version of the message. Never <code>null</code>.
     * @throws MessagingException
     */
    public Message storeSmallMessage(final Message message, final Runnable runnable) throws MessagingException {
        return this.localStore.database.execute(true, new DbCallback<Message>() {
            @Override
            public Message doDbWork(final SQLiteDatabase db) throws WrappedException, UnavailableStorageException {
                try {
                    appendMessages(new Message[] { message });
                    final String uid = message.getUid();
                    final Message result = getMessage(uid);
                    runnable.run();
                    // Set a flag indicating this message has now be fully downloaded
                    result.setFlag(Flag.X_DOWNLOADED_FULL, true);
                    return result;
                } catch (MessagingException e) {
                    throw new WrappedException(e);
                }
            }
        });
    }

    /**
     * The method differs slightly from the contract; If an incoming message already has a uid
     * assigned and it matches the uid of an existing message then this message will replace the
     * old message. It is implemented as a delete/insert. This functionality is used in saving
     * of drafts and re-synchronization of updated server messages.
     *
     * NOTE that although this method is located in the LocalStore class, it is not guaranteed
     * that the messages supplied as parameters are actually {@link LocalMessage} instances (in
     * fact, in most cases, they are not). Therefore, if you want to make local changes only to a
     * message, retrieve the appropriate local message instance first (if it already exists).
     */
    @Override
    public Map<String, String> appendMessages(Message[] messages) throws MessagingException {
        return appendMessages(messages, false);
    }

    public void destroyMessages(final Message[] messages) {
        try {
            this.localStore.database.execute(true, new DbCallback<Void>() {
                @Override
                public Void doDbWork(final SQLiteDatabase db) throws WrappedException, UnavailableStorageException {
                    for (Message message : messages) {
                        try {
                            message.destroy();
                        } catch (MessagingException e) {
                            throw new WrappedException(e);
                        }
                    }
                    return null;
                }
            });
        } catch (MessagingException e) {
            throw new WrappedException(e);
        }
    }

    private ThreadInfo getThreadInfo(SQLiteDatabase db, String messageId, boolean onlyEmpty) {
    	WhereBuilder whereClause = WhereBuilder.cond().folderId(mFolderId).and().messageId(messageId);
    	if (onlyEmpty)
    		whereClause.and().isEmpty();
		Cursor cursor = select(new JoinBuilder(MESSAGES).lJoin(THREADS).on(MESSAGES, ID, THREADS, MESSAGE_ID))
    			.cols(THREAD_COLS).where(whereClause)
    			.orderBy(MESSAGES + "." + ID, true).limit(1).execute(db);

        if (cursor != null) {
            try {
                if (cursor.getCount() > 0) {
                    cursor.moveToFirst();
                    long threadId = cursor.getLong(0);
                    long msgId = cursor.getLong(1);
                    long rootId = (cursor.isNull(2)) ? -1 : cursor.getLong(2);
                    long parentId = (cursor.isNull(3)) ? -1 : cursor.getLong(3);

                    return new LocalStore.ThreadInfo(threadId, msgId, messageId, rootId, parentId);
                }
            } finally {
                cursor.close();
            }
        }

        return null;
    }

    /**
     * The method differs slightly from the contract; If an incoming message already has a uid
     * assigned and it matches the uid of an existing message then this message will replace
     * the old message. This functionality is used in saving of drafts and re-synchronization
     * of updated server messages.
     *
     * NOTE that although this method is located in the LocalStore class, it is not guaranteed
     * that the messages supplied as parameters are actually {@link LocalMessage} instances (in
     * fact, in most cases, they are not). Therefore, if you want to make local changes only to a
     * message, retrieve the appropriate local message instance first (if it already exists).
     * @param messages
     * @param copy
     * @return Map<String, String> uidMap of srcUids -> destUids
     */
    private Map<String, String> appendMessages(final Message[] messages, final boolean copy) throws MessagingException {
        open(OPEN_MODE_RW);
        try {
            final Map<String, String> uidMap = new HashMap<String, String>();
            this.localStore.database.execute(true, new DbCallback<Void>() {
                @Override
                public Void doDbWork(final SQLiteDatabase db) throws WrappedException, UnavailableStorageException {
                    try {
                        for (Message message : messages) {
                            if (!(message instanceof MimeMessage)) {
                                throw new Error("LocalStore can only store Messages that extend MimeMessage");
                            }

                            long oldMessageId = -1;
                            String uid = message.getUid();
                            if (uid == null || copy) {
                                /*
                                 * Create a new message in the database
                                 */
                                String randomLocalUid = K9.LOCAL_UID_PREFIX +
                                        UUID.randomUUID().toString();

                                if (copy) {
                                    // Save mapping: source UID -> target UID
                                    uidMap.put(uid, randomLocalUid);
                                } else {
                                    // Modify the Message instance to reference the new UID
                                    message.setUid(randomLocalUid);
                                }

                                // The message will be saved with the newly generated UID
                                uid = randomLocalUid;
                            } else {
                                /*
                                 * Replace an existing message in the database
                                 */
                                LocalMessage oldMessage = getMessage(uid);

                                if (oldMessage != null) {
                                    oldMessageId = oldMessage.getId();
                                }

                                deleteAttachments(message.getUid());
                            }

                            long rootId = -1;
                            long parentId = -1;

                            if (oldMessageId == -1) {
                                // This is a new message. Do the message threading.
                                ThreadInfo threadInfo = doMessageThreading(db, message);
                                oldMessageId = threadInfo.msgId;
                                rootId = threadInfo.rootId;
                                parentId = threadInfo.parentId;
                            }

                            boolean isDraft = (message.getHeader(K9.IDENTITY_HEADER) != null);

                            List<Part> attachments;
                            String text;
                            String html;
                            if (isDraft) {
                                // Don't modify the text/plain or text/html part of our own
                                // draft messages because this will cause the values stored in
                                // the identity header to be wrong.
                                ViewableContainer container =
                                        MimeUtility.extractPartsFromDraft(message);

                                text = container.text;
                                html = container.html;
                                attachments = container.attachments;
                            } else {
                                ViewableContainer container =
                                        MimeUtility.extractTextAndAttachments(LocalFolder.this.localStore.mApplication, message);

                                attachments = container.attachments;
                                text = container.text;
                                html = HtmlConverter.convertEmoji2Img(container.html);
                            }

                            String preview = Message.calculateContentPreview(text);

                            try {
                            	ContentValues cv = map().put("uid", uid).put("subject", message.getSubject())
                            		.putAddrList("sender_list", message.getFrom()).putDate(DATE, message.getSentDate())
                            		.putFlags(message.getFlags()).putFlag("deleted", message, Flag.DELETED)
                            		.putFlag("read", message, Flag.SEEN).putFlag("flagged", message, Flag.FLAGGED)
                            		.putFlag("answered", message, Flag.ANSWERED).putFlag("forwarded", message, Flag.FORWARDED)
                            		.put(FOLDER_ID, mFolderId).putAddrList("to_list", message.getRecipients(RecipientType.TO))
                            		.putAddrList("cc_list", message.getRecipients(RecipientType.CC))
                            		.putAddrList("bcc_list", message.getRecipients(RecipientType.BCC))
                            		.putText("html_content", html).putText("text_content", text).put("preview", preview)
                            		.putAddrList("reply_to_list", message.getReplyTo()).put("attachment_count", attachments.size())
                            		.putDate("internal_date", message.getInternalDate()).put("mime_type", message.getMimeType())
                            		.put("empty", 0).putNotNull(MESSAGE_ID, message.getMessageId()).build();

                                long msgId;
                                if (oldMessageId == -1) {
                                	msgId = insert(MESSAGES).data(cv).execute(db);
                                    // Create entry in 'threads' table
                                    insert(THREADS).data(map().put(MESSAGE_ID, msgId).putU("root", rootId, -1).putU("parent", parentId, -1).build()).execute(db);
                                } else {
                                	update(MESSAGES).data(cv).where(WhereBuilder.cond().id(oldMessageId)).execute(db);
                                    msgId = oldMessageId;
                                }

                                for (Part attachment : attachments) {
                                    saveAttachment(msgId, attachment, copy);
                                }
                                saveHeaders(msgId, (MimeMessage)message);
                            } catch (Exception e) {
                                throw new MessagingException("Error appending message", e);
                            }
                        }
                    } catch (MessagingException e) {
                        throw new WrappedException(e);
                    }
                    return null;
                }
            });

            this.localStore.notifyChange();

            return uidMap;
        } catch (WrappedException e) {
            throw(MessagingException) e.getCause();
        }
    }

    /**
     * Update the given message in the LocalStore without first deleting the existing
     * message (contrast with appendMessages). This method is used to store changes
     * to the given message while updating attachments and not removing existing
     * attachment data.
     * TODO In the future this method should be combined with appendMessages since the Message
     * contains enough data to decide what to do.
     * @param message
     * @throws MessagingException
     */
    public void updateMessage(final LocalMessage message) throws MessagingException {
        open(OPEN_MODE_RW);
        try {
            this.localStore.database.execute(false, new DbCallback<Void>() {
                @Override
                public Void doDbWork(final SQLiteDatabase db) throws WrappedException, UnavailableStorageException {
                    try {
                        message.buildMimeRepresentation();

                        ViewableContainer container =
                                MimeUtility.extractTextAndAttachments(LocalFolder.this.localStore.mApplication, message);

                        List<Part> attachments = container.attachments;
                        String text = container.text;
                        String html = HtmlConverter.convertEmoji2Img(container.html);

                        String preview = Message.calculateContentPreview(text);

                        try {
                        	update(MESSAGES).data(map().put("uid", message.getUid()).put("subject", message.getSubject())
                					.putAddrList("sender_list", message.getFrom()).putDate(DATE, message.getSentDate())
                					.putFlags(message.getFlags()).put(FOLDER_ID, mFolderId)
                					.putAddrList("to_list", message.getRecipients(RecipientType.TO))
                					.putAddrList("cc_list", message.getRecipients(RecipientType.CC))
                        			.putAddrList("bcc_list", message.getRecipients(RecipientType.BCC))
                        			.putText("html_content", html).putText("text_content", text)
                        			.putText("preview", preview).putAddrList("reply_to_list", message.getReplyTo())
                        			.put("attachment_count", attachments.size()).putFlag("read", message, Flag.SEEN)
                        			.putFlag("flagged", message, Flag.FLAGGED).putFlag("answerd", message, Flag.ANSWERED)
                        			.putFlag("forwarded", message, Flag.FORWARDED).build())
                			.where(WhereBuilder.cond().id(message.mId)).execute(db);

                            for (int i = 0, count = attachments.size(); i < count; i++) {
                                Part attachment = attachments.get(i);
                                saveAttachment(message.mId, attachment, false);
                            }
                            saveHeaders(message.getId(), message);
                        } catch (Exception e) {
                            throw new MessagingException("Error appending message", e);
                        }
                    } catch (MessagingException e) {
                        throw new WrappedException(e);
                    }
                    return null;
                }
            });
        } catch (WrappedException e) {
            throw(MessagingException) e.getCause();
        }

        this.localStore.notifyChange();
    }

    /**
     * Save the headers of the given message. Note that the message is not
     * necessarily a {@link LocalMessage} instance.
     * @param id
     * @param message
     * @throws com.fsck.k9.mail.MessagingException
     */
    private void saveHeaders(final long id, final MimeMessage message) throws MessagingException {
        this.localStore.database.execute(true, new DbCallback<Void>() {
            @Override
            public Void doDbWork(final SQLiteDatabase db) throws WrappedException, UnavailableStorageException {

                deleteHeaders(id);
                for (String name : message.getHeaderNames()) {
                        String[] values = message.getHeader(name);
                        for (String value : values) {
                        	insert(HEADERS).data(map().put(MESSAGE_ID, id).put(NAME, name).put("value", value).build()).execute(db);
                        }
                }

                // Remember that all headers for this message have been saved, so it is
                // not necessary to download them again in case the user wants to see all headers.
                Flag[] flags = message.getFlags();
                int appendPos = flags.length;
				Flag[] appendedFlags = new Flag[appendPos + 1];
                System.arraycopy(flags, 0, appendedFlags, 0, appendPos);
                appendedFlags[appendPos] = Flag.X_GOT_ALL_HEADERS;
                update(MESSAGES).data(map().putFlags(appendedFlags).build()).where(WhereBuilder.cond().id(id)).execute(db);
                return null;
            }
        });
    }

    void deleteHeaders(final long id) throws UnavailableStorageException {
        this.localStore.database.execute(false, new DbCallback<Void>() {
            @Override
            public Void doDbWork(final SQLiteDatabase db) throws WrappedException, UnavailableStorageException {
            	QueryBuilder.delete(HEADERS).where(WhereBuilder.cond().messageId(id)).execute(db);
                return null;
            }
        });
    }

    private void saveAttachment(final long messageId, final Part attachment, final boolean saveAsNew)
    throws IOException, MessagingException {
        try {
            this.localStore.database.execute(true, new DbCallback<Void>() {
                @Override
                public Void doDbWork(final SQLiteDatabase db) throws WrappedException, UnavailableStorageException {
                    try {
                        long attachmentId = -1;
                        Uri contentUri = null;
                        int size = -1;
                        File tempAttachmentFile = null;

                        if ((!saveAsNew) && (attachment instanceof LocalAttachmentBodyPart)) {
                            attachmentId = ((LocalAttachmentBodyPart) attachment).getAttachmentId();
                        }

                        final File attachmentDirectory = StorageManager.getInstance(LocalFolder.this.localStore.mApplication).getAttachmentDirectory(LocalFolder.this.localStore.uUid, LocalFolder.this.localStore.database.getStorageProviderId());
                        if (attachment.getBody() != null) {
                            Body body = attachment.getBody();
                            if (body instanceof LocalAttachmentBody) {
                                contentUri = ((LocalAttachmentBody) body).getContentUri();
                            } else if (body instanceof Message) {
                                // It's a message, so use Message.writeTo() to output the
                                // message including all children.
                                Message message = (Message) body;
                                tempAttachmentFile = File.createTempFile("att", null, attachmentDirectory);
                                FileOutputStream out = new FileOutputStream(tempAttachmentFile);
                                try {
                                    message.writeTo(out);
                                } finally {
                                    out.close();
                                }
                                size = (int) (tempAttachmentFile.length() & 0x7FFFFFFFL);
                            } else {
                                /*
                                 * If the attachment has a body we're expected to save it into the local store
                                 * so we copy the data into a cached attachment file.
                                 */
                                InputStream in = attachment.getBody().getInputStream();
                                try {
                                    tempAttachmentFile = File.createTempFile("att", null, attachmentDirectory);
                                    FileOutputStream out = new FileOutputStream(tempAttachmentFile);
                                    try {
                                        size = IOUtils.copy(in, out);
                                    } finally {
                                        out.close();
                                    }
                                } finally {
                                    try { in.close(); } catch (Throwable ignore) {}
                                }
                            }
                        }

                        if (size == -1) {
                            /*
                             * If the attachment is not yet downloaded see if we can pull a size
                             * off the Content-Disposition.
                             */
                            String disposition = attachment.getDisposition();
                            if (disposition != null) {
                                String sizeParam = MimeUtility.getHeaderParameter(disposition, "size");
                                if (sizeParam != null) {
                                    try {
                                        size = Integer.parseInt(sizeParam);
                                    } catch (NumberFormatException e) { /* Ignore */ }
                                }
                            }
                        }
                        if (size == -1) {
                            size = 0;
                        }

                        String storeData =
                            Utility.combine(attachment.getHeader(
                                                MimeHeader.HEADER_ANDROID_ATTACHMENT_STORE_DATA), ',');

                        String name = MimeUtility.getHeaderParameter(attachment.getContentType(), "name");
                        String contentId = MimeUtility.getHeaderParameter(attachment.getContentId(), null);

                        String contentDisposition = MimeUtility.unfoldAndDecode(attachment.getDisposition());
                        String dispositionType = contentDisposition;

                        if (dispositionType != null) {
                            int pos = dispositionType.indexOf(';');
                            if (pos != -1) {
                                // extract the disposition-type, "attachment", "inline" or extension-token (see the RFC 2183)
                                dispositionType = dispositionType.substring(0, pos);
                            }
                        }

                        if (name == null && contentDisposition != null) {
                            name = MimeUtility.getHeaderParameter(contentDisposition, "filename");
                        }
                        if (attachmentId == -1) {
                        	attachmentId = insert(ATTACHMENTS).data(map().put(MESSAGE_ID, messageId)
                        			.putToS("content_uri", contentUri).put("store_data", storeData).put("size", size)
                        			.put(NAME, name).put("mime_type", attachment.getMimeType()).put("content_id", contentId)
                        			.put("content_disposition", dispositionType).build()).execute(db);
                        } else {
                        	update(ATTACHMENTS).data(map().putToS("content_uri", contentUri).put("size", size).build())
                        	.where(WhereBuilder.cond().id(attachmentId)).execute(db);
                        }

                        if (attachmentId != -1 && tempAttachmentFile != null) {
                            File attachmentFile = new File(attachmentDirectory, Long.toString(attachmentId));
                            tempAttachmentFile.renameTo(attachmentFile);
                            contentUri = AttachmentProvider.getAttachmentUri(
                                             mAccount,
                                             attachmentId);
                            if (MimeUtil.isMessage(attachment.getMimeType())) {
                                attachment.setBody(new LocalAttachmentMessageBody(
                                        contentUri, LocalFolder.this.localStore.mApplication));
                            } else {
                                attachment.setBody(new LocalAttachmentBody(
                                        contentUri, LocalFolder.this.localStore.mApplication));
                            }
                            update(ATTACHMENTS).data(map().putToS("content_uri", contentUri).build())
                            .where(WhereBuilder.cond().id(attachmentId)).execute(db);
                        }

                        /* The message has attachment with Content-ID */
                        if (contentId != null && contentUri != null) {
                            Cursor cursor = select(MESSAGES).cols("html_content").where(WhereBuilder.cond().id(messageId)).execute(db);
                            try {
                                if (cursor.moveToNext()) {
                                    String htmlContent = cursor.getString(0);

                                    if (htmlContent != null) {
                                        String newHtmlContent = htmlContent.replaceAll(
                                                                    Pattern.quote("cid:" + contentId),
                                                                    contentUri.toString());
                                        update(MESSAGES).data(map().put("html_content", newHtmlContent).build())
                                        .where(WhereBuilder.cond().id(messageId)).execute(db);
                                    }
                                }
                            } finally {
                                Utility.closeQuietly(cursor);
                            }
                        }

                        if (attachmentId != -1 && attachment instanceof LocalAttachmentBodyPart) {
                            ((LocalAttachmentBodyPart) attachment).setAttachmentId(attachmentId);
                        }
                        return null;
                    } catch (MessagingException e) {
                        throw new WrappedException(e);
                    } catch (IOException e) {
                        throw new WrappedException(e);
                    }
                }
            });
        } catch (WrappedException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }

            throw (MessagingException) cause;
        }
    }

    /**
     * Changes the stored uid of the given message (using it's internal id as a key) to
     * the uid in the message.
     * @param message
     * @throws com.fsck.k9.mail.MessagingException
     */
    public void changeUid(final LocalMessage message) throws MessagingException {
        open(OPEN_MODE_RW);
        this.localStore.database.execute(false, new DbCallback<Void>() {
            @Override
            public Void doDbWork(final SQLiteDatabase db) throws WrappedException, UnavailableStorageException {
            	update(MESSAGES).data(map().put("uid", message.getUid()).build())
            	.where(WhereBuilder.cond().id(message.mId)).execute(db);
                return null;
            }
        });

        //TODO: remove this once the UI code exclusively uses the database id
        this.localStore.notifyChange();
    }

    @Override
    public void setFlags(final Message[] messages, final Flag[] flags, final boolean value)
    throws MessagingException {
        open(OPEN_MODE_RW);

        // Use one transaction to set all flags
        try {
            this.localStore.database.execute(true, new DbCallback<Void>() {
                @Override
                public Void doDbWork(final SQLiteDatabase db) throws WrappedException,
                        UnavailableStorageException {

                    for (Message message : messages) {
                        try {
                            message.setFlags(flags, value);
                        } catch (MessagingException e) {
                            Log.e(K9.LOG_TAG, "Something went wrong while setting flag", e);
                        }
                    }

                    return null;
                }
            });
        } catch (WrappedException e) {
            throw(MessagingException) e.getCause();
        }
    }

    @Override
    public void setFlags(Flag[] flags, boolean value) throws MessagingException {
        open(OPEN_MODE_RW);
        for (Message message : getMessages(null)) {
            message.setFlags(flags, value);
        }
    }

    @Override
    public String getUidFromMessageId(Message message) throws MessagingException {
        throw new MessagingException("Cannot call getUidFromMessageId on LocalFolder");
    }

    public void clearMessagesOlderThan(long cutoff) throws MessagingException {
        open(OPEN_MODE_RO);

        SelectBuilder query = select(new JoinBuilder(MESSAGES).lJoin(THREADS).on(MESSAGES, ID, THREADS, MESSAGE_ID))
        		.cols(MESSAGES_COLS).where(WhereBuilder.cond().notEmpty().and().folderId(mFolderId).and().dateBefore(cutoff));
        Message[] messages = this.localStore.getMessages(null, this, query);

        for (Message message : messages) {
            message.destroy();
        }

        this.localStore.notifyChange();
    }

    public void clearAllMessages() throws MessagingException {
        open(OPEN_MODE_RO);
        try {
            this.localStore.database.execute(false, new DbCallback<Void>() {
                @Override
                public Void doDbWork(final SQLiteDatabase db) throws WrappedException {
                    try {
                        // Get UIDs for all messages to delete
                        Cursor cursor = select(MESSAGES).cols("uid").where(WhereBuilder.cond().folderId(mFolderId).and().notEmpty()).execute(db);
                        try {
                            // Delete attachments of these messages
                            while (cursor.moveToNext()) {
                                deleteAttachments(cursor.getString(0));
                            }
                        } finally {
                            cursor.close();
                        }

                        // Delete entries in 'threads' and 'messages'
                        QueryBuilder.delete(THREADS).where(WhereBuilder.cond().inSubselect(MESSAGE_ID, select(MESSAGES).cols(ID).where(WhereBuilder.cond().folderId(mFolderId)))).execute(db);
                        QueryBuilder.delete(MESSAGES).where(WhereBuilder.cond().folderId(mFolderId)).execute(db);
                        return null;
                    } catch (MessagingException e) {
                        throw new WrappedException(e);
                    }
                }
            });
        } catch (WrappedException e) {
            throw(MessagingException) e.getCause();
        }

        this.localStore.notifyChange();

        setPushState(null);
        setLastPush(0);
        setLastChecked(0);
        setVisibleLimit(mAccount.getDisplayCount());
    }

    @Override
    public void delete(final boolean recurse) throws MessagingException {
        try {
            this.localStore.database.execute(false, new DbCallback<Void>() {
                @Override
                public Void doDbWork(final SQLiteDatabase db) throws WrappedException, UnavailableStorageException {
                    try {
                        // We need to open the folder first to make sure we've got it's id
                        open(OPEN_MODE_RO);
                        Message[] messages = getMessages(null);
                        for (Message message : messages) {
                            deleteAttachments(message.getUid());
                        }
                    } catch (MessagingException e) {
                        throw new WrappedException(e);
                    }
                    QueryBuilder.delete(FOLDERS).where(WhereBuilder.cond().id(mFolderId)).execute(db);
                    return null;
                }
            });
        } catch (WrappedException e) {
            throw(MessagingException) e.getCause();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof LocalFolder) {
            return ((LocalFolder)o).mName.equals(mName);
        }
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return mName.hashCode();
    }

    void deleteAttachments(final long messageId) throws MessagingException {
        open(OPEN_MODE_RW);
        this.localStore.database.execute(false, new DbCallback<Void>() {
            @Override
            public Void doDbWork(final SQLiteDatabase db) throws WrappedException, UnavailableStorageException {
                Cursor attachmentsCursor = null;
                try {
                    String accountUuid = mAccount.getUuid();
                    Context context = LocalFolder.this.localStore.mApplication;

                    // Get attachment IDs
                    attachmentsCursor = select(ATTACHMENTS).cols(ID).where(WhereBuilder.cond().messageId(messageId)).execute(db);

                    final File attachmentDirectory = StorageManager.getInstance(LocalFolder.this.localStore.mApplication)
                            .getAttachmentDirectory(LocalFolder.this.localStore.uUid, LocalFolder.this.localStore.database.getStorageProviderId());

                    while (attachmentsCursor.moveToNext()) {
                        String attachmentId = Long.toString(attachmentsCursor.getLong(0));
                        try {
                            // Delete stored attachment
                            File file = new File(attachmentDirectory, attachmentId);
                            if (file.exists()) {
                                file.delete();
                            }

                            // Delete thumbnail file
                            AttachmentProvider.deleteThumbnail(context, accountUuid,
                                    attachmentId);
                        } catch (Exception e) { /* ignore */ }
                    }

                    // Delete attachment metadata from the database
                    QueryBuilder.delete(ATTACHMENTS).where(WhereBuilder.cond().messageId(messageId)).execute(db);
                } finally {
                    Utility.closeQuietly(attachmentsCursor);
                }
                return null;
            }
        });
    }

    private void deleteAttachments(final String uid) throws MessagingException {
        open(OPEN_MODE_RW);
        try {
            this.localStore.database.execute(false, new DbCallback<Void>() {
                @Override
                public Void doDbWork(final SQLiteDatabase db) throws WrappedException, UnavailableStorageException {
                    Cursor messagesCursor = null;
                    try {
                        messagesCursor = select(MESSAGES).cols(ID).where(WhereBuilder.cond().folderId(mFolderId).and().uId(uid)).execute(db);
                        while (messagesCursor.moveToNext()) {
                            long messageId = messagesCursor.getLong(0);
                            deleteAttachments(messageId);
                        }
                    } catch (MessagingException e) {
                        throw new WrappedException(e);
                    } finally {
                        Utility.closeQuietly(messagesCursor);
                    }
                    return null;
                }
            });
        } catch (WrappedException e) {
            throw(MessagingException) e.getCause();
        }
    }

    @Override
    public boolean isInTopGroup() {
        return mInTopGroup;
    }

    public void setInTopGroup(boolean inTopGroup) throws MessagingException {
        mInTopGroup = inTopGroup;
        updateFolderColumn(TOP_GROUP, mInTopGroup ? 1 : 0);
    }

    public Integer getLastUid() {
        return mLastUid;
    }

    /**
     * <p>Fetches the most recent <b>numeric</b> UID value in this folder.  This is used by
     * {@link com.fsck.k9.controller.MessagingController#shouldNotifyForMessage} to see if messages being
     * fetched are new and unread.  Messages are "new" if they have a UID higher than the most recent UID prior
     * to synchronization.</p>
     *
     * <p>This only works for protocols with numeric UIDs (like IMAP). For protocols with
     * alphanumeric UIDs (like POP), this method quietly fails and shouldNotifyForMessage() will
     * always notify for unread messages.</p>
     *
     * <p>Once Issue 1072 has been fixed, this method and shouldNotifyForMessage() should be
     * updated to use internal dates rather than UIDs to determine new-ness. While this doesn't
     * solve things for POP (which doesn't have internal dates), we can likely use this as a
     * framework to examine send date in lieu of internal date.</p>
     * @throws MessagingException
     */
    public void updateLastUid() throws MessagingException {
        Integer lastUid = this.localStore.database.execute(false, new DbCallback<Integer>() {
            @Override
            public Integer doDbWork(final SQLiteDatabase db) {
                Cursor cursor = null;
                try {
                    open(OPEN_MODE_RO);
                    cursor = select(MESSAGES).cols("MAX(uid)").where(WhereBuilder.cond().folderId(mFolderId)).execute(db);
                    if (cursor.getCount() > 0) {
                        cursor.moveToFirst();
                        return cursor.getInt(0);
                    }
                } catch (Exception e) {
                    Log.e(K9.LOG_TAG, "Unable to updateLastUid: ", e);
                } finally {
                    Utility.closeQuietly(cursor);
                }
                return null;
            }
        });
        if (K9.DEBUG)
            Log.d(K9.LOG_TAG, "Updated last UID for folder " + mName + " to " + lastUid);
        mLastUid = lastUid;
    }

    public Long getOldestMessageDate() throws MessagingException {
        return this.localStore.database.execute(false, new DbCallback<Long>() {
            @Override
            public Long doDbWork(final SQLiteDatabase db) {
                Cursor cursor = null;
                try {
                    open(OPEN_MODE_RO);
                    cursor = select(MESSAGES).cols("MIN(date)").where(WhereBuilder.cond().folderId(mFolderId)).execute(db);
                    if (cursor.getCount() > 0) {
                        cursor.moveToFirst();
                        return cursor.getLong(0);
                    }
                } catch (Exception e) {
                    Log.e(K9.LOG_TAG, "Unable to fetch oldest message date: ", e);
                } finally {
                    Utility.closeQuietly(cursor);
                }
                return null;
            }
        });
    }

    private ThreadInfo doMessageThreading(SQLiteDatabase db, Message message)
            throws MessagingException {
        long rootId = -1;
        long parentId = -1;

        String messageId = message.getMessageId();

        // If there's already an empty message in the database, update that
        ThreadInfo msgThreadInfo = getThreadInfo(db, messageId, true);

        // Get the message IDs from the "References" header line
        String[] referencesArray = message.getHeader("References");
        List<String> messageIds = null;
        if (referencesArray != null && referencesArray.length > 0) {
            messageIds = Utility.extractMessageIds(referencesArray[0]);
        }

        // Append the first message ID from the "In-Reply-To" header line
        String[] inReplyToArray = message.getHeader("In-Reply-To");
        String inReplyTo = null;
        if (inReplyToArray != null && inReplyToArray.length > 0) {
            inReplyTo = Utility.extractMessageId(inReplyToArray[0]);
            if (inReplyTo != null) {
                if (messageIds == null) {
                    messageIds = new ArrayList<String>(1);
                    messageIds.add(inReplyTo);
                } else if (!messageIds.contains(inReplyTo)) {
                    messageIds.add(inReplyTo);
                }
            }
        }

        if (messageIds == null) {
            // This is not a reply, nothing to do for us.
            return (msgThreadInfo != null) ?
                    msgThreadInfo : new LocalStore.ThreadInfo(-1, -1, messageId, -1, -1);
        }

        for (String reference : messageIds) {
            ThreadInfo threadInfo = getThreadInfo(db, reference, false);

            if (threadInfo == null) {
                // Create placeholder message in 'messages' table
                long newMsgId = insert(MESSAGES).data(map().put(MESSAGE_ID, reference).put(FOLDER_ID, mFolderId).put("empty", 1).build()).execute(db);

                // Create entry in 'threads' table
                parentId = insert(THREADS).data(map().put(MESSAGE_ID, newMsgId).putU("root", rootId, -1).putU("parent", parentId, -1).build()).execute(db);
                if (rootId == -1) {
                    rootId = parentId;
                }
            } else {
                if (rootId != -1 && threadInfo.rootId == -1 && rootId != threadInfo.threadId) {
                    // We found an existing root container that is not
                    // the root of our current path (References).
                    // Connect it to the current parent.

                    // Let all children know who's the new root
                    update(THREADS).data(map().put("root", rootId).build()).where(WhereBuilder.cond().root(threadInfo.threadId)).execute(db);

                    // Connect the message to the current parent
                    update(THREADS).data(map().put("parent", parentId).build()).where(WhereBuilder.cond().id(threadInfo.threadId)).execute(db);
                } else {
                    rootId = (threadInfo.rootId == -1) ?
                            threadInfo.threadId : threadInfo.rootId;
                }
                parentId = threadInfo.threadId;
            }
        }

        //TODO: set in-reply-to "link" even if one already exists

        long threadId;
        long msgId;
        if (msgThreadInfo != null) {
            threadId = msgThreadInfo.threadId;
            msgId = msgThreadInfo.msgId;
        } else {
            threadId = -1;
            msgId = -1;
        }

        return new LocalStore.ThreadInfo(threadId, msgId, messageId, rootId, parentId);
    }

    public List<Message> extractNewMessages(final List<Message> messages)
            throws MessagingException {

        try {
            return this.localStore.database.execute(false, new DbCallback<List<Message>>() {
                @Override
                public List<Message> doDbWork(final SQLiteDatabase db) throws WrappedException {
                    try {
                        open(OPEN_MODE_RW);
                    } catch (MessagingException e) {
                        throw new WrappedException(e);
                    }

                    List<Message> result = new ArrayList<Message>();

                    Set<String> existingMessages = new HashSet<String>();
                    int start = 0;

                    while (start < messages.size()) {
                    	List<String> uids = new ArrayList<String>();

                        int count = Math.min(messages.size() - start, LocalStore.UID_CHECK_BATCH_SIZE);

                        for (int i = start, end = start + count; i < end; i++) {
                            uids.add(messages.get(i).getUid());
                        }
                        Cursor cursor = select(MESSAGES).cols(LocalStore.UID_CHECK_PROJECTION).where(WhereBuilder.cond().folderId(mFolderId).and().inUids(uids)).execute(db);
                        		
                        try {
                            while (cursor.moveToNext()) {
                                String uid = cursor.getString(0);
                                existingMessages.add(uid);
                            }
                        } finally {
                            Utility.closeQuietly(cursor);
                        }

                        for (int i = start, end = start + count; i < end; i++) {
                            Message message = messages.get(i);
                            if (!existingMessages.contains(message.getUid())) {
                                result.add(message);
                            }
                        }

                        existingMessages.clear();
                        start += count;
                    }

                    return result;
                }
            });
        } catch (WrappedException e) {
            throw(MessagingException) e.getCause();
        }
    }
}