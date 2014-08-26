package com.fsck.k9.mail.store.local;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;

import com.fsck.k9.helper.StringUtils;

public class QueryBuilder {
	
	// Tables
	public static final String FOLDERS = "folders";
	public static final String THREADS = "threads";
	public static final String MESSAGES = "messages";
	
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

    // Threads Columns
    public static final String MESSAGE_ID = "message_id";
    
    // Message Columns
    public static final String CONTENT_DATA = "html_content, text_content, mime_type";
    public static final String FOLDER_ID = "folder_id";
    
    // Attachment Columns
    public static final String ATTACH_DATA = "id, size, name, mime_type, store_data, content_uri, content_id, content_disposition";
    
    // Header Columns
    public static final String HEADER_DATA = "message_id, name, value";
    
    private static final String FOLDER_COLS =
            "folders.id, name, visible_limit, last_updated, status, push_state, last_pushed, " +
            "integrate, top_group, poll_class, push_class, display_class";

    private static final String MESSAGES_COLS =
            "subject, sender_list, date, uid, flags, messages.id, to_list, cc_list, " +
            "bcc_list, reply_to_list, attachment_count, internal_date, messages.message_id, " +
            "folder_id, preview, threads.id, threads.root, deleted, read, flagged, answered, " +
            "forwarded";

	private StringBuilder queryBuffer = new StringBuilder();
	private List<String> argList = new ArrayList<String>();
	
	public static QueryBuilder query() {
		return new QueryBuilder();
	}
	
	public static UpdateQueryBuilder update(String table) {
		return new QueryBuilder().new UpdateQueryBuilder(table);
	}
	
	public static InsertQueryBuilder insert(String table) {
		return new QueryBuilder().new InsertQueryBuilder(table);
	}

	public static String dot(String... parts) {
		if (parts == null || parts.length == 0)
			return null;
		StringBuilder buffer = new StringBuilder(parts[0]);
		for (int i = 1; i < parts.length; i++) {
			buffer.append(".").append(parts[i]);
		}
		return buffer.toString();
	}
	
	public class InsertQueryBuilder {

		public InsertQueryBuilder(String table) {
			queryBuffer.append("INSERT INTO ").append(table).append("(");
		}
		
		public InsertQueryBuilder set(String column, String value) {
			return setInternal(column, value);
		}
		
		public InsertQueryBuilder set(String column, long value) {
			return setInternal(column, Long.toString(value));
		}
		
		public InsertQueryBuilder set(String column, long value, long unless) {
			if (value != unless)
				return set(column, value);
			else
				return this;
		}
		
		private InsertQueryBuilder setInternal(String column, String value) {
			queryBuffer.append(column).append(", ");
			argList.add(value);
			return this;
		}
		
		public void execute(SQLiteDatabase db) {
			queryBuffer.delete(queryBuffer.length() - 2, queryBuffer.length());
			queryBuffer.append(") VALUES (").append(argList.get(0));
			for (int i = 1; i < argList.size(); i++) {
				queryBuffer.append(", ").append(argList.get(i));
			}
			queryBuffer.append(")");
			db.rawQuery(queryBuffer.toString(), new String[0]);
		}
		
	}
	
	public class UpdateQueryBuilder extends WhereClauseBuilder {
		
		private boolean firstValue = true;
		
		UpdateQueryBuilder(String table) {
			queryBuffer.append("UPDATE ").append(table).append(" SET");
		}
		
		public UpdateQueryBuilder with(String field, long value) {
			return with(field, Long.toString(value));
		}
		
		public UpdateQueryBuilder with(String field, String value) {
			if (firstValue)
				firstValue = false;
			else
				queryBuffer.append(",");
			queryBuffer.append(" " + field + " = ?");
			argList.add(value);
			return this;
		}

	}
	
	public class JoinClauseBuilder extends WhereClauseBuilder {
		
		public JoinClauseBuilder lJoin(String table) {
			queryBuffer.append(" LEFT JOIN ").append(table);
			return this;
		}
		
		public JoinClauseBuilder on(String leftField, String rightField) {
			queryBuffer.append(" ON ").append(leftField).append(" = ").append(rightField);
			return this;
		}
		
	}
	
	public class WhereClauseBuilder extends OrderClauseBuilder {
		
		public WhereClauseBuilder where() {
			queryBuffer.append(" WHERE ");
			return this;
		}
		
		public WhereClauseBuilder folder(String name, long id) {
			return name != null ? folder(name) : id(id);
		}
		
		public WhereClauseBuilder folder(String name) {
			return addWhere("name", name);
		}
		
		public WhereClauseBuilder id(long mId) {
			return addWhere(ID, Long.toString(mId));
		}

		public WhereClauseBuilder folderId(long folderId) {
			return addWhere(FOLDER_ID, folderId);
		}

		public WhereClauseBuilder messageId(long mId) {
			return addWhere(MESSAGE_ID, mId);
		}
		
		public WhereClauseBuilder uId(String uId) {
			return addWhere("uid", uId);
		}
		
		public WhereClauseBuilder dateBefore(long cutoff) {
			queryBuffer.append("date < ").append(Long.toString(cutoff));
			return this;
		}
		
		public WhereClauseBuilder notEmpty() {
			queryBuffer.append("(empty IS NULL OR empty != 1)");
			return this;
		}
		
		public WhereClauseBuilder notDeleted() {
			return flag("deleted", false);
		}
		
		public WhereClauseBuilder notRead() {
			return flag("read", false);
		}
		
		public WhereClauseBuilder flagged() {
			return flag("flagged", true);
		}
		
		public WhereClauseBuilder inMessages(Collection<Long> messageIds) {
			queryBuffer.append(MESSAGE_ID).append(" IN (");
			for (Long id : messageIds) {
				queryBuffer.append("?, ");
				argList.add(id.toString());
			}
			int qbLength = queryBuffer.length();
			queryBuffer.delete(qbLength - 2, qbLength);
			queryBuffer.append(")");
			return this;
		}
		
		public WhereClauseBuilder andLiteral(String literalClause, List<String> arguments) {
			if (!StringUtils.isNullOrEmpty(literalClause)) {
				queryBuffer.append(" AND (").append(literalClause).append(")");
				argList.addAll(arguments);
			}
			return this;
		}
		
		public WhereClauseBuilder and() {
			queryBuffer.append(" AND ");
			return this;
		}
		
		private WhereClauseBuilder addWhere(String field, String argValue) {
			queryBuffer.append(field).append(" = ?");
			argList.add(argValue);
			return this;
		}
		
		private WhereClauseBuilder addWhere(String field, long argValue) {
			return addWhere(field, Long.toString(argValue));
		}
		
		private WhereClauseBuilder flag(String flag, boolean set) {
			queryBuffer.append(flag).append(" = ").append(set ? 1 : 0);
			return this;
		}
		
	}
	
	public class OrderClauseBuilder extends LimitClauseBuilder {
		
		public LimitClauseBuilder byId() {
			return byFieldAscending(ID);
		}
		
		public LimitClauseBuilder byName() {
			return byFieldAscending("name");
		}
		
		private LimitClauseBuilder byFieldAscending(String field) {
			queryBuffer.append(" ORDER BY ").append(field).append(" ASC");
			return this;
		}
		
		public LimitClauseBuilder byDateDown() {
			queryBuffer.append(" ORDER BY ").append("date").append(" DESC");
			return this;
		}
		
	}
	
	public class LimitClauseBuilder extends ExecutableQuery {
		
		public ExecutableQuery limit(int limit) {
			queryBuffer.append(" LIMIT ").append(limit);
			return this;
		}
		
		public ExecutableQuery limit(int limit, int offset) {
			queryBuffer.append(" LIMIT ").append(limit).append(" OFFSET ").append(offset);
			return this;
		}
		
	}

	public class ExecutableQuery {
		
		public Cursor toCursor(SQLiteDatabase db) {
			return db.rawQuery(queryBuffer.toString(), argList.toArray(new String[argList.size()]));
		}
		
		public void execute(SQLiteDatabase db) {
			toCursor(db);
		}

		@Override
		public String toString() {
			return queryBuffer.toString();
		}
		
	}
	
	private QueryBuilder() {
	}
	
	public JoinClauseBuilder folderData() {
		return folderData(FOLDER_COLS);
	}
	
	public JoinClauseBuilder folderData(String cols) {
		return selectStmt(FOLDERS, cols);
	}
	
	public JoinClauseBuilder messageCount() {
		return messageData("COUNT(id)");
	}
	
	public JoinClauseBuilder messageData() {
		return messageData(MESSAGES_COLS);
	}
	
	public JoinClauseBuilder messageData(String cols) {
		return selectStmt(MESSAGES, cols);
	}
	
	public JoinClauseBuilder attachmentData() {
		return selectStmt("attachments", ATTACH_DATA);
	}

	public JoinClauseBuilder headers() {
		return selectStmt("headers", HEADER_DATA);
	}
	
	private JoinClauseBuilder selectStmt(String table, String cols) {
		queryBuffer.append("SELECT ").append(cols).append(" FROM ").append(table);
		return new JoinClauseBuilder();
	}
	
}
