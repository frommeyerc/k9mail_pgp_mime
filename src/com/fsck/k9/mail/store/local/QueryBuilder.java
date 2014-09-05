package com.fsck.k9.mail.store.local;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;

import com.fsck.k9.helper.StringUtils;
import com.fsck.k9.mail.Address;
import com.fsck.k9.mail.Flag;
import com.fsck.k9.mail.Message;

public class QueryBuilder {

    public static final String HEADERS = "headers";
    // Tables
    public static final String FOLDERS = "folders";
    public static final String THREADS = "threads";
    public static final String MESSAGES = "messages";
    public static final String ATTACHMENTS = "attachments";

    // General Columns
    public static final String ID = "id";

    // Folder Columns
    public static final String LAST_UPDATE = "last_updated";
    public static final String LAST_PUSH = "last_pushed";
    public static final String VISIBLE_LIMIT = "visible_limit";
    public static final String STATUS = "status";
    public static final String PUSH_STATE = "push_state";
    public static final String DISPLAY_CLASS = "display_class";
    public static final String POLL_CLASS = "poll_class";
    public static final String PUSH_CLASS = "push_class";
    public static final String INTEGRATE = "integrate";
    public static final String TOP_GROUP = "top_group";

    public static final String DATE = "date";

    public static final String NAME = "name";

    // Threads Columns
    public static final String MESSAGE_ID = "message_id";

    // Message Columns
    public static final String[] CONTENT_DATA = { "html_content",
            "text_content", "mime_type" };
    public static final String FOLDER_ID = "folder_id";

    // Attachment Columns
    public static final String ATTACH_DATA = "id, size, name, mime_type, store_data, content_uri, content_id, content_disposition";

    // Header Columns
    public static final String HEADER_DATA = "message_id, name, value";

    public static final String[] FOLDER_COLS = { "folders.id", "name",
            "visible_limit", "last_updated", "status", "push_state",
            "last_pushed", "integrate", "top_group", "poll_class",
            "push_class", "display_class" };

    public static final String[] MESSAGES_COLS = { "subject", "sender_list",
            "date", "uid", "flags", "messages.id", "to_list", "cc_list",
            "bcc_list", "reply_to_list", "attachment_count", "internal_date",
            "messages.message_id", "folder_id", "preview", "threads.id",
            "threads.root", "deleted", "read", "flagged", "answered",
            "forwarded" };

    public static final String[] THREAD_COLS = { "threads.id",
            "threads.message_id", "threads.root", "threads.parent" };

    public static SelectBuilder select(String table) {
        return new SelectBuilder(table);
    }

    public static JoinSelectBuilder select(JoinBuilder builder) {
        return new JoinSelectBuilder(builder);
    }

    public static UpdateBuilder update(String table) {
        return new UpdateBuilder(table);
    }

    public static InsertBuilder insert(String table) {
        return new InsertBuilder(table);
    }

    public static DeleteBuilder delete(String table) {
        return new DeleteBuilder(table);
    }

    public static abstract class BaseQuery {

        private String table;

        public BaseQuery(String table) {
            this.table = table;
        }

        protected String getTable() {
            return table;
        }

    }

    public static class DeleteBuilder extends BaseQuery implements
            UsesWhere<DeleteBuilder> {

        private String whereClause = null;
        private String[] whereArgs = null;

        public DeleteBuilder(String table) {
            super(table);
        }

        @Override
        public DeleteBuilder where(WhereBuilder b) {
            this.whereClause = b.buildClause();
            this.whereArgs = b.getArgs();
            return this;
        }

        public int execute(SQLiteDatabase db) {
            return db.delete(getTable(), whereClause, whereArgs);
        }

    }

    public static class InsertBuilder extends BaseQuery implements
            ColumnDataConsumer<InsertBuilder> {

        private ContentValues data = null;

        public InsertBuilder(String table) {
            super(table);
        }

        @Override
        public InsertBuilder data(ContentValues data) {
            this.data = data;
            return this;
        }

        public long execute(SQLiteDatabase db) {
            return db.insert(getTable(), null, data);
        }

    }

    public static class UpdateBuilder extends BaseQuery implements
            UsesWhere<UpdateBuilder>, ColumnDataConsumer<UpdateBuilder> {

        private String whereClause = null;
        private String[] whereArgs = null;
        private ContentValues data = null;

        public UpdateBuilder(String table) {
            super(table);
        }

        @Override
        public UpdateBuilder where(WhereBuilder b) {
            whereClause = b.buildClause();
            whereArgs = b.getArgs();
            return this;
        }

        @Override
        public UpdateBuilder data(ContentValues data) {
            this.data = data;
            return this;
        }

        public int execute(SQLiteDatabase db) {
            return db.update(getTable(), data, whereClause, whereArgs);
        }

    }

    public static class SelectBuilder extends BaseQuery implements
            UsesWhere<SelectBuilder> {

        protected String[] columns = null;

        protected String whereClause = null;
        protected String[] whereArgs = null;

        protected String groupBy = null;
        protected String having = null;
        protected String orderBy = null;
        protected String limit = null;

        public SelectBuilder(String table) {
            super(table);
        }

        public SelectBuilder cols(String... columns) {
            this.columns = columns;
            return this;
        }

        @Override
        public SelectBuilder where(WhereBuilder b) {
            whereClause = b.buildClause();
            whereArgs = b.getArgs();
            return this;
        }

        public SelectBuilder orderBy(String column, boolean up) {
            this.orderBy = column + " " + (up ? "ASC" : "DESC");
            return this;
        }

        public SelectBuilder limit(int limit) {
            this.limit = "LIMIT " + Integer.toString(limit);
            return this;
        }

        public SelectBuilder limit(int limit, int offset) {
            this.limit = "LIMIT " + limit + " OFFSET " + offset;
            return this;
        }

        public Cursor execute(SQLiteDatabase db) {
            return db.query(getTable(), columns, whereClause, whereArgs,
                    groupBy, having, orderBy, limit);
        }

        String toQuery() {
            return "SELECT " + join(columns) + " FROM " + getTable()
                    + " WHERE " + whereClause;
        }

        String[] getArgs() {
            return whereArgs;
        }

        private String join(String[] cols) {
            if (cols == null || cols.length == 0)
                return "";
            StringBuilder b = new StringBuilder(cols[0]);
            for (int i = 1; i < cols.length; i++)
                b.append(", ").append(cols[i]);
            return b.toString();
        }

    }

    public static class JoinSelectBuilder extends SelectBuilder {

        public JoinSelectBuilder(JoinBuilder builder) {
            super(builder.build());
        }

        @Override
        public Cursor execute(SQLiteDatabase db) {
            SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
            queryBuilder.setTables(getTable());
            return queryBuilder.query(db, columns, whereClause, whereArgs,
                    groupBy, having, orderBy, limit);
        }

    }

    public interface UsesWhere<T> {

        T where(WhereBuilder b);

    }

    public interface ColumnDataConsumer<T> {

        T data(ContentValues data);

    }

    public static class DataBuilder {

        private ContentValues data = new ContentValues();

        public static DataBuilder map() {
            return new DataBuilder();
        }

        public ContentValues build() {
            return data;
        }

        public DataBuilder put(String column, String value) {
            data.put(column, value);
            return this;
        }

        public DataBuilder putNotNull(String column, String value) {
            if (value != null)
                data.put(column, value);
            return this;
        }

        public DataBuilder put(String column, long value) {
            data.put(column, value);
            return this;
        }

        public DataBuilder putU(String column, long value, long unless) {
            if (value != unless)
                data.put(column, value);
            return this;
        }

        public DataBuilder putNull(String column) {
            data.putNull(column);
            return this;
        }

        public DataBuilder putDate(String column, Date date) {
            data.put(column,
                    date != null ? date.getTime() : System.currentTimeMillis());
            return this;
        }

        public DataBuilder putFlags(Flag[] flags) {
            data.put("flags", LocalStoreUtil.serializeFlags(flags));
            return this;
        }

        public DataBuilder putFlag(String column, Message message, Flag flag) {
            data.put(column, message.isSet(flag) ? 1 : 0);
            return this;
        }

        public DataBuilder putAddrList(String column, Address[] addresses) {
            data.put(column, Address.pack(addresses));
            return this;
        }

        public DataBuilder putText(String column, String text) {
            data.put(column, text.isEmpty() ? null : text);
            return this;
        }

        public DataBuilder putToS(String column, Object value) {
            data.put(column, value != null ? value.toString() : null);
            return this;
        }

    }

    public static class WhereBuilder {

        private StringBuilder queryBuffer = new StringBuilder();
        private List<String> argList = new ArrayList<String>();

        public static WhereBuilder cond() {
            return new WhereBuilder();
        }

        public WhereBuilder folder(String name, long id) {
            return name != null ? folder(name) : id(id);
        }

        public WhereBuilder folder(String name) {
            return addWhere(NAME, name);
        }

        public WhereBuilder id(long mId) {
            return addWhere(ID, Long.toString(mId));
        }

        public WhereBuilder folderId(long folderId) {
            return addWhere(MESSAGES + "." + FOLDER_ID, folderId);
        }

        public WhereBuilder messageId(long mId) {
            return addWhere(MESSAGE_ID, mId);
        }

        public WhereBuilder messageId(String mId) {
            return addWhere(MESSAGES + "." + MESSAGE_ID, mId);
        }

        public WhereBuilder uId(String uId) {
            return addWhere("uid", uId);
        }

        public WhereBuilder root(long rootId) {
            return addWhere("root", rootId);
        }

        public WhereBuilder dateBefore(long cutoff) {
            queryBuffer.append("date < ").append(Long.toString(cutoff));
            return this;
        }

        public WhereBuilder isEmpty() {
            queryBuffer.append("messages.empty = 1");
            return this;
        }

        public WhereBuilder notEmpty() {
            queryBuffer.append("(empty IS NULL OR empty != 1)");
            return this;
        }

        public WhereBuilder notDeleted() {
            return flag("deleted", false);
        }

        public WhereBuilder notRead() {
            return flag("read", false);
        }

        public WhereBuilder flagged() {
            return flag("flagged", true);
        }

        public WhereBuilder inMessages(Collection<Long> messageIds) {
            return addInClause(MESSAGE_ID, messageIds);
        }

        public WhereBuilder inUids(Collection<String> uids) {
            return addInClause("uid", uids);
        }

        private WhereBuilder addInClause(String field,
                Collection<? extends Object> values) {
            queryBuffer.append(field).append(" IN (");
            for (Object id : values) {
                queryBuffer.append("?, ");
                argList.add(id.toString());
            }
            int qbLength = queryBuffer.length();
            queryBuffer.delete(qbLength - 2, qbLength);
            queryBuffer.append(")");
            return this;
        }

        public WhereBuilder inSubselect(String field, SelectBuilder builder) {
            queryBuffer.append(field).append(" IN (").append(builder.toQuery())
                    .append(")");
            argList.addAll(Arrays.asList(builder.getArgs()));
            return this;
        }

        public WhereBuilder andLiteral(String literalClause,
                List<String> arguments) {
            if (!StringUtils.isNullOrEmpty(literalClause)) {
                queryBuffer.append(" AND (").append(literalClause).append(")");
                argList.addAll(arguments);
            }
            return this;
        }

        public WhereBuilder and() {
            queryBuffer.append(" AND ");
            return this;
        }

        private WhereBuilder addWhere(String field, String argValue) {
            queryBuffer.append(field).append(" = ?");
            argList.add(argValue);
            return this;
        }

        private WhereBuilder addWhere(String field, long argValue) {
            return addWhere(field, Long.toString(argValue));
        }

        private WhereBuilder flag(String flag, boolean set) {
            queryBuffer.append(flag).append(" = ").append(set ? 1 : 0);
            return this;
        }

        public String buildClause() {
            return queryBuffer.toString();
        }

        public String[] getArgs() {
            return argList.toArray(new String[argList.size()]);
        }

    }

    public static class JoinBuilder {

        private StringBuilder queryBuffer = new StringBuilder();

        public JoinBuilder(String table) {
            queryBuffer.append(table);
        }

        public JoinBuilder lJoin(String table) {
            queryBuffer.append(" LEFT JOIN ").append(table);
            return this;
        }

        public JoinBuilder on(String leftTable, String leftField,
                String rightTable, String rightField) {
            queryBuffer.append(" ON ").append(leftTable).append(".")
                    .append(leftField).append(" = ").append(rightTable)
                    .append(".").append(rightField);
            return this;
        }

        public String build() {
            return queryBuffer.toString();
        }

    }

}
