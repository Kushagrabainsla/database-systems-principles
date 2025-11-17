/************************************************************
	Project#1:	CLP & DDL
 ************************************************************/

#include "db.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdint.h>
#include <errno.h>

#if defined(_WIN32) || defined(_WIN64)
#define strcasecmp _stricmp
#endif



/* Globals to store output column ordering/widths for JOIN printing */
static int join_out_count = 0;
static int join_out_widths[MAX_NUM_COL * 2];
static int join_out_types[MAX_NUM_COL * 2];
static char join_out_names[MAX_NUM_COL * 2][MAX_IDENT_LEN+8];

static int open_tab_rw(const char *table_name, FILE **pf, table_file_header *hdr)
{
	char fname[MAX_IDENT_LEN + 5] = {0};
	snprintf(fname, sizeof(fname), "%s.tab", table_name);

	*pf = fopen(fname, "rb+");         // read/write binary
	if (!*pf) return FILE_OPEN_ERROR;

	fseek(*pf, 0, SEEK_SET);
	if (fread(hdr, sizeof(*hdr), 1, *pf) != 1) {
		fclose(*pf); *pf = NULL;
		return FILE_OPEN_ERROR;
	}
	return 0;
}

static int write_header(FILE *f, const table_file_header *hdr_in)
{
	table_file_header on_disk = *hdr_in;
	on_disk.tpd_ptr = 0; // zero when writing

	/* Recompute on-disk file_size to reflect actual number of records */
	on_disk.file_size = on_disk.record_offset + on_disk.record_size * on_disk.num_records;

	fseek(f, 0, SEEK_SET);
	if (fwrite(&on_disk, sizeof(on_disk), 1, f) != 1) return FILE_WRITE_ERROR;
	fflush(f);
	return 0;
}

static long row_pos(const table_file_header *hdr, int row_idx)
{
  	return (long)hdr->record_offset + (long)row_idx * (long)hdr->record_size;
}

static inline int round4(int n) { return (n + 3) & ~3; }

static int compute_record_size_from_tpd(const tpd_entry *tpd) {
	int rec = 0;
	const cd_entry *col = (const cd_entry*)((const char*)tpd + tpd->cd_offset);
	for (int i = 0; i < tpd->num_columns; ++i, ++col) {
		if (col->col_type == T_INT) rec += 1 + 4; // 1-byte length + 4
		else rec += 1 + col->col_len; // CHAR/VARCHAR(n)
	}
	return round4(rec);
}

static int create_table_data_file(const tpd_entry *tpd) {
	char fname[MAX_IDENT_LEN + 5] = {0};
	snprintf(fname, sizeof(fname), "%s.tab", tpd->table_name);

	int rec_size = compute_record_size_from_tpd(tpd);

	table_file_header hdr = {0};
	hdr.record_size    = rec_size;
	hdr.num_records    = 0;
	hdr.record_offset  = sizeof(table_file_header);
	/* Initially only write the header; do not pre-allocate space for MAX_ROWS */
	hdr.file_size      = hdr.record_offset; 
	hdr.file_header_flag = 0;
	hdr.tpd_ptr        = 0; // zero on disk

	/* Write only the header to create a small initial file. File will grow as records are inserted. */
	FILE *fh = fopen(fname, "wb");
	if (!fh) return FILE_OPEN_ERROR;
	if (fwrite(&hdr, sizeof(hdr), 1, fh) != 1) { fclose(fh); return FILE_WRITE_ERROR; }
	fflush(fh);
	fclose(fh);
	return 0;
}

static int drop_table_data_file(const char *table_name) {
	char fname[MAX_IDENT_LEN + 5] = {0};
	snprintf(fname, sizeof(fname), "%s.tab", table_name);
	if (remove(fname) == 0) return 0;
	if (errno == ENOENT)    return 0;
	return FILE_OPEN_ERROR;
}


/* Extract a field value from a row buffer at the specified column index */
static void extract_field_at_column(unsigned char *row_buffer, cd_entry *columns, int col_index, unsigned char *str_value, int *int_value, unsigned char *length)
{
	int offset = 0;
	
	// Calculate offset to the target column
	for (int i = 0; i < col_index; i++) {
		offset += 1 + ((columns[i].col_type == T_INT) ? 4 : columns[i].col_len);
	}
	
	// Read length byte
	*length = row_buffer[offset++];
	
	// Read the actual value
	if (columns[col_index].col_type == T_INT) {
		if (*length > 0) {
			memcpy(int_value, row_buffer + offset, 4);
		} else {
			*int_value = 0; // NULL
		}
	} else {
		if (*length > 0) {
			memcpy(str_value, row_buffer + offset, *length);
		}
	}
}

/* Compare two field values for equality */
static bool are_fields_equal(cd_entry *col, unsigned char len1, void *val1, unsigned char len2, void *val2)
{
	// Both NULL
	if (len1 == 0 && len2 == 0) return true;
	
	// One NULL, one not
	if (len1 == 0 || len2 == 0) return false;
	
	// Compare based on type
	if (col->col_type == T_INT) {
		return *(int32_t*)val1 == *(int32_t*)val2;
	} else {
		return (len1 == len2) && (memcmp(val1, val2, len1) == 0);
	}
}

/**
 * Print a single field value
 */
static void print_field(cd_entry *col, unsigned char length, void *value)
{
	if (length == 0) {
		printf("NULL");
	} else if (col->col_type == T_INT) {
		printf("%d", *(int32_t*)value);
	} else {
		/* Print strings without surrounding quotes */
		printf("%.*s", (int)length, (char*)value);
	}
}

/**
 * Find common columns between two tables for NATURAL JOIN
 */
static int find_common_columns(tpd_entry *tpd1, tpd_entry *tpd2, int *col_map1, int *col_map2)
{
	cd_entry *cols1 = (cd_entry*)((char*)tpd1 + tpd1->cd_offset);
	cd_entry *cols2 = (cd_entry*)((char*)tpd2 + tpd2->cd_offset);
	int num_common = 0;
	
	for (int i = 0; i < tpd1->num_columns; i++) {
		for (int j = 0; j < tpd2->num_columns; j++) {
			if (strcmp(cols1[i].col_name, cols2[j].col_name) == 0) {
				col_map1[num_common] = i;
				col_map2[num_common] = j;
				num_common++;
				break;
			}
		}
	}
	
	return num_common;
}

/**
 * Print the header row for NATURAL JOIN result
 * Format: common columns | remaining table1 columns | remaining table2 columns
 */
static void print_join_header(tpd_entry *tpd1, tpd_entry *tpd2,  int *common_map1, int *common_map2, int num_common)
{
	cd_entry *cols1 = (cd_entry*)((char*)tpd1 + tpd1->cd_offset);
	cd_entry *cols2 = (cd_entry*)((char*)tpd2 + tpd2->cd_offset);
	bool first = true;

	/* We'll build an ordered list of output columns and compute widths */
	int out_col_count = 0;
	static int out_col_widths[MAX_NUM_COL * 2];
	static int out_col_types[MAX_NUM_COL * 2];
	static char out_col_names[MAX_NUM_COL * 2][MAX_IDENT_LEN+8];

	// helper to add a column to output list
	auto add_out_col = [&](const char *name, int col_type, int col_len) {
		strncpy(out_col_names[out_col_count], name, sizeof(out_col_names[0]) - 1);
		out_col_names[out_col_count][sizeof(out_col_names[0]) - 1] = '\0';
		out_col_types[out_col_count] = col_type;
		int w = (int)strlen(name);
		if (col_type == T_INT) {
			if (w < 5) w = 5; // width for ints
		} else {
			if (w < col_len) w = col_len;
		}
		out_col_widths[out_col_count] = w;
		out_col_count++;
	};

	// Print common columns first (and add to out list)
	for (int i = 0; i < num_common; i++) {
		add_out_col(cols1[common_map1[i]].col_name, cols1[common_map1[i]].col_type, cols1[common_map1[i]].col_len);
	}

	// Print remaining columns from table1
	for (int i = 0; i < tpd1->num_columns; i++) {
		bool is_common = false;
		for (int c = 0; c < num_common; c++) {
			if (common_map1[c] == i) {
				is_common = true;
				break;
			}
		}
		if (!is_common) {
			add_out_col(cols1[i].col_name, cols1[i].col_type, cols1[i].col_len);
			first = false;
		}
	}
	
	// Print remaining columns from table2
	for (int i = 0; i < tpd2->num_columns; i++) {
		bool is_common = false;
		for (int c = 0; c < num_common; c++) {
			if (common_map2[c] == i) {
				is_common = true;
				break;
			}
		}
		if (!is_common) {
			add_out_col(cols2[i].col_name, cols2[i].col_type, cols2[i].col_len);
			first = false;
		}
	}
    
	/* Print header row with computed widths */
	for (int i = 0; i < out_col_count; ++i) {
		printf("%-*s", out_col_widths[i], out_col_names[i]);
		if (i + 1 < out_col_count) printf(" ");
	}
	printf("\n");

	/* Print separator line */
	for (int i = 0; i < out_col_count; ++i) {
		for (int j = 0; j < out_col_widths[i]; ++j) putchar('-');
		if (i + 1 < out_col_count) putchar(' ');
	}
	printf("\n");

	/* Store generated widths and counts in static globals for use by print_joined_row */
	/* We'll copy into static arrays accessible by print_joined_row */
	/* Copy into file-scoped join_out_* arrays so print_joined_row can use them */
	join_out_count = out_col_count;
	for (int i = 0; i < out_col_count; ++i) {
		join_out_widths[i] = out_col_widths[i];
		join_out_types[i] = out_col_types[i];
		strncpy(join_out_names[i], out_col_names[i], sizeof(join_out_names[0]) - 1);
		join_out_names[i][sizeof(join_out_names[0]) - 1] = '\0';
	}
}

/* Check if two rows match on all common columns */
static bool rows_match_on_common_columns(unsigned char *row1, unsigned char *row2, cd_entry *cols1, cd_entry *cols2, int *common_map1, int *common_map2, int num_common)
{
	for (int c = 0; c < num_common; c++) {
		int idx1 = common_map1[c];
		int idx2 = common_map2[c];
		
		unsigned char len1, len2;
		unsigned char str_val1[256] = {0}, str_val2[256] = {0};
		int int_val1 = 0, int_val2 = 0;
		
		extract_field_at_column(row1, cols1, idx1, str_val1, &int_val1, &len1);
		extract_field_at_column(row2, cols2, idx2, str_val2, &int_val2, &len2);
		
		void *v1 = (cols1[idx1].col_type == T_INT) ? (void*)&int_val1 : (void*)str_val1;
		void *v2 = (cols2[idx2].col_type == T_INT) ? (void*)&int_val2 : (void*)str_val2;
		
		if (!are_fields_equal(&cols1[idx1], len1, v1, len2, v2)) {
			return false;
		}
	}
	
	return true;
}

/**
 * Print a joined row with proper column ordering
 */
static void print_joined_row(unsigned char *row1, unsigned char *row2, tpd_entry *tpd1, tpd_entry *tpd2, int *common_map1, int *common_map2, int num_common)
{
	cd_entry *cols1 = (cd_entry*)((char*)tpd1 + tpd1->cd_offset);
	cd_entry *cols2 = (cd_entry*)((char*)tpd2 + tpd2->cd_offset);
	int pos = 0;

	/* Print common columns (from table1) */
	for (int c = 0; c < num_common; c++) {
		int idx = common_map1[c];
		unsigned char len;
		unsigned char str_val[256] = {0};
		int int_val = 0;
		extract_field_at_column(row1, cols1, idx, str_val, &int_val, &len);

		if (join_out_types[pos] == T_INT) {
			if (len == 0) printf("%-*s", join_out_widths[pos], "NULL");
			else printf("%*d", join_out_widths[pos], int_val);
		} else {
			if (len == 0) printf("%-*s", join_out_widths[pos], "NULL");
			else printf("%-*.*s", join_out_widths[pos], (int)len, (char*)str_val);
		}
		if (pos + 1 < join_out_count) printf(" ");
		pos++;
	}

	/* Remaining columns from table1 */
	for (int i = 0; i < tpd1->num_columns; i++) {
		bool is_common = false;
		for (int c = 0; c < num_common; c++) {
			if (common_map1[c] == i) { is_common = true; break; }
		}
		if (!is_common) {
			unsigned char len;
			unsigned char str_val[256] = {0};
			int int_val = 0;
			extract_field_at_column(row1, cols1, i, str_val, &int_val, &len);

			if (join_out_types[pos] == T_INT) {
				if (len == 0) printf("%-*s", join_out_widths[pos], "NULL");
				else printf("%*d", join_out_widths[pos], int_val);
			} else {
				if (len == 0) printf("%-*s", join_out_widths[pos], "NULL");
				else printf("%-*.*s", join_out_widths[pos], (int)len, (char*)str_val);
			}
			if (pos + 1 < join_out_count) printf(" ");
			pos++;
		}
	}

	/* Remaining columns from table2 */
	for (int i = 0; i < tpd2->num_columns; i++) {
		bool is_common = false;
		for (int c = 0; c < num_common; c++) {
			if (common_map2[c] == i) { is_common = true; break; }
		}
		if (!is_common) {
			unsigned char len;
			unsigned char str_val[256] = {0};
			int int_val = 0;
			extract_field_at_column(row2, cols2, i, str_val, &int_val, &len);

			if (join_out_types[pos] == T_INT) {
				if (len == 0) printf("%-*s", join_out_widths[pos], "NULL");
				else printf("%*d", join_out_widths[pos], int_val);
			} else {
				if (len == 0) printf("%-*s", join_out_widths[pos], "NULL");
				else printf("%-*.*s", join_out_widths[pos], (int)len, (char*)str_val);
			}
			if (pos + 1 < join_out_count) printf(" ");
			pos++;
		}
	}

	printf("\n");
}

int main(int argc, char** argv)
{
	int rc = 0;
	token_list *tok_list=NULL, *tok_ptr=NULL, *tmp_tok_ptr=NULL;

	if ((argc != 2) || (strlen(argv[1]) == 0))
	{
		printf("Usage: db \"command statement\"\n");
		return 1;
	}

	rc = initialize_tpd_list();

  if (rc)
  {
		printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
  }
	else
	{
    rc = get_token(argv[1], &tok_list);

		/* Test code */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			printf("%16s \t%d \t %d\n",tok_ptr->tok_string, tok_ptr->tok_class,
				      tok_ptr->tok_value);
			tok_ptr = tok_ptr->next;
		}
    
		if (!rc)
		{
			rc = do_semantic(tok_list);
		}

		if (rc)
		{
			tok_ptr = tok_list;
			while (tok_ptr != NULL)
			{
				if ((tok_ptr->tok_class == error) ||
					  (tok_ptr->tok_value == INVALID))
				{
					printf("\nError in the string: %s\n", tok_ptr->tok_string);
					printf("rc=%d\n", rc);
					break;
				}
				tok_ptr = tok_ptr->next;
			}
		}

    /* Whether the token list is valid or not, we need to free the memory */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
      tmp_tok_ptr = tok_ptr->next;
      free(tok_ptr);
      tok_ptr=tmp_tok_ptr;
		}
	}

	return rc;
}

/************************************************************* 
	This is a lexical analyzer for simple SQL statements
 *************************************************************/
int get_token(char* command, token_list** tok_list)
{
	int rc=0,i,j;
	char *start, *cur, temp_string[MAX_TOK_LEN];
	bool done = false;
	
	start = cur = command;
	while (!done)
	{
		bool found_keyword = false;

		/* This is the TOP Level for each token */
	  memset ((void*)temp_string, '\0', MAX_TOK_LEN);
		i = 0;

		/* Get rid of all the leading blanks */
		while (*cur == ' ')
			cur++;

		if (cur && isalpha(*cur))
		{
			// find valid identifier
			int t_class;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((isalnum(*cur)) || (*cur == '_'));

			if (!(strchr(STRING_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank, (, ), or a comma, then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{

				// We have an identifier with at least 1 character
				// Now check if this ident is a keyword
				for (j = 0, found_keyword = false; j < TOTAL_KEYWORDS_PLUS_TYPE_NAMES; j++)
				{
					if ((strcasecmp(keyword_table[j], temp_string) == 0))
					{
						found_keyword = true;
						break;
					}
				}

				if (found_keyword)
				{
				  if (KEYWORD_OFFSET+j < K_CREATE)
						t_class = type_name;
					else if (KEYWORD_OFFSET+j >= F_SUM)
            t_class = function_name;
          else
					  t_class = keyword;

					add_to_list(tok_list, temp_string, t_class, KEYWORD_OFFSET+j);
				}
				else
				{
					if (strlen(temp_string) <= MAX_IDENT_LEN)
					  add_to_list(tok_list, temp_string, identifier, IDENT);
					else
					{
						add_to_list(tok_list, temp_string, error, INVALID);
						rc = INVALID;
						done = true;
					}
				}

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if (isdigit(*cur))
		{
			// find valid number
			do 
			{
				temp_string[i++] = *cur++;
			}
			while (isdigit(*cur));

			if (!(strchr(NUMBER_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank or a ), then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{
				add_to_list(tok_list, temp_string, constant, INT_LITERAL);

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if ((*cur == '(') || (*cur == ')') || (*cur == ',') || (*cur == '*')
		         || (*cur == '=') || (*cur == '<') || (*cur == '>'))
		{
			/* Catch all the symbols here. Note: no look ahead here. */
			int t_value;
			switch (*cur)
			{
				case '(' : t_value = S_LEFT_PAREN; break;
				case ')' : t_value = S_RIGHT_PAREN; break;
				case ',' : t_value = S_COMMA; break;
				case '*' : t_value = S_STAR; break;
				case '=' : t_value = S_EQUAL; break;
				case '<' : t_value = S_LESS; break;
				case '>' : t_value = S_GREATER; break;
			}

			temp_string[i++] = *cur++;

			add_to_list(tok_list, temp_string, symbol, t_value);

			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
		}
    else if (*cur == '\'')
    {
      /* Find STRING_LITERRAL */
			int t_class;
      cur++;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((*cur) && (*cur != '\''));

      temp_string[i] = '\0';

			if (!*cur)
			{
				/* If we reach the end of line */
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
      else /* must be a ' */
      {
        add_to_list(tok_list, temp_string, constant, STRING_LITERAL);
        cur++;
				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
        }
      }
    }
		else
		{
			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
			else
			{
				/* not a ident, number, or valid symbol */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
		}
	}
			
  return rc;
}

void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value)
{
	token_list *cur = *tok_list;
	token_list *ptr = NULL;

	// printf("%16s \t%d \t %d\n",tmp, t_class, t_value);

	ptr = (token_list*)calloc(1, sizeof(token_list));
	strcpy(ptr->tok_string, tmp);
	ptr->tok_class = t_class;
	ptr->tok_value = t_value;
	ptr->next = NULL;

  if (cur == NULL)
		*tok_list = ptr;
	else
	{
		while (cur->next != NULL)
			cur = cur->next;

		cur->next = ptr;
	}
	return;
}

int do_semantic(token_list *tok_list)
{
	int rc = 0, cur_cmd = INVALID_STATEMENT;
	bool unique = false;
  token_list *cur = tok_list;

	if ((cur->tok_value == K_CREATE) &&
			((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("CREATE TABLE statement\n");
		cur_cmd = CREATE_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_DROP) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("DROP TABLE statement\n");
		cur_cmd = DROP_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("LIST TABLE statement\n");
		cur_cmd = LIST_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_SCHEMA)))
	{
		printf("LIST SCHEMA statement\n");
		cur_cmd = LIST_SCHEMA;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_INSERT) &&
			(cur->next != NULL) && (cur->next->tok_value == K_INTO))
	{
		printf("INSERT statement\n");
		cur_cmd = INSERT;            // uses your enum (104)
		cur = cur->next->next;       // point at <table_name>
	}
	else if ((cur->tok_value == K_SELECT) &&
			(cur->next != NULL) && (cur->next->tok_value == S_STAR) &&
			(cur->next->next != NULL) && (cur->next->next->tok_value == K_FROM))
	{
		printf("SELECT * statement\n");
		cur_cmd = SELECT_STAR;            // uses your enum (107)
		cur = cur->next;             // pass pointer starting at '*'
	}
	else
  {
		printf("Invalid statement\n");
		rc = cur_cmd;
	}

	if (cur_cmd != INVALID_STATEMENT)
	{
		switch(cur_cmd)
		{
			case CREATE_TABLE:
						rc = sem_create_table(cur);
						break;
			case DROP_TABLE:
						rc = sem_drop_table(cur);
						break;
			case LIST_TABLE:
						rc = sem_list_tables();
						break;
			case LIST_SCHEMA:
						rc = sem_list_schema(cur);
						break;
			case INSERT:
				rc = sem_insert_into(cur);
				break;
			case SELECT_STAR:
				rc = sem_select_star(cur);
				break;
			default:
					; /* no action */
		}
	}
	
	return rc;
}

int sem_create_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry tab_entry;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry	col_entry[MAX_NUM_COL];


	memset(&tab_entry, '\0', sizeof(tpd_entry));
	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if ((new_entry = get_tpd_from_list(cur->tok_string)) != NULL)
		{
			rc = DUPLICATE_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			strcpy(tab_entry.table_name, cur->tok_string);
			cur = cur->next;
			if (cur->tok_value != S_LEFT_PAREN)
			{
				//Error
				rc = INVALID_TABLE_DEFINITION;
				cur->tok_value = INVALID;
			}
			else
			{
				memset(&col_entry, '\0', (MAX_NUM_COL * sizeof(cd_entry)));

				/* Now build a set of column entries */
				cur = cur->next;
				do
				{
					if ((cur->tok_class != keyword) &&
							(cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_COLUMN_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						int i;
						for(i = 0; i < cur_id; i++)
						{
              /* make column name case sensitive */
							if (strcmp(col_entry[i].col_name, cur->tok_string)==0)
							{
								rc = DUPLICATE_COLUMN_NAME;
								cur->tok_value = INVALID;
								break;
							}
						}

						if (!rc)
						{
							strcpy(col_entry[cur_id].col_name, cur->tok_string);
							col_entry[cur_id].col_id = cur_id;
							col_entry[cur_id].not_null = false;    /* set default */

							cur = cur->next;
							if (cur->tok_class != type_name)
							{
								// Error
								rc = INVALID_TYPE_NAME;
								cur->tok_value = INVALID;
							}
							else
							{
                /* Set the column type here, int or char */
								col_entry[cur_id].col_type = cur->tok_value;
								cur = cur->next;
		
								if (col_entry[cur_id].col_type == T_INT)
								{
									if ((cur->tok_value != S_COMMA) &&
										  (cur->tok_value != K_NOT) &&
										  (cur->tok_value != S_RIGHT_PAREN))
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
								  else
									{
										col_entry[cur_id].col_len = sizeof(int);
										
										if ((cur->tok_value == K_NOT) &&
											  (cur->next->tok_value != K_NULL))
										{
											rc = INVALID_COLUMN_DEFINITION;
											cur->tok_value = INVALID;
										}	
										else if ((cur->tok_value == K_NOT) &&
											    (cur->next->tok_value == K_NULL))
										{					
											col_entry[cur_id].not_null = true;
											cur = cur->next->next;
										}
	
										if (!rc)
										{
											/* I must have either a comma or right paren */
											if ((cur->tok_value != S_RIGHT_PAREN) &&
												  (cur->tok_value != S_COMMA))
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
		                  {
												if (cur->tok_value == S_RIGHT_PAREN)
												{
 													column_done = true;
												}
												cur = cur->next;
											}
										}
									}
								}   // end of T_INT processing
								else
								{
									// It must be char() or varchar() 
									if (cur->tok_value != S_LEFT_PAREN)
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										/* Enter char(n) processing */
										cur = cur->next;
		
										if (cur->tok_value != INT_LITERAL)
										{
											rc = INVALID_COLUMN_LENGTH;
											cur->tok_value = INVALID;
										}
										else
										{
											/* Got a valid integer - convert */
											col_entry[cur_id].col_len = atoi(cur->tok_string);
											cur = cur->next;
											
											if (cur->tok_value != S_RIGHT_PAREN)
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												cur = cur->next;
						
												if ((cur->tok_value != S_COMMA) &&
														(cur->tok_value != K_NOT) &&
														(cur->tok_value != S_RIGHT_PAREN))
												{
													rc = INVALID_COLUMN_DEFINITION;
													cur->tok_value = INVALID;
												}
												else
												{
													if ((cur->tok_value == K_NOT) &&
														  (cur->next->tok_value != K_NULL))
													{
														rc = INVALID_COLUMN_DEFINITION;
														cur->tok_value = INVALID;
													}
													else if ((cur->tok_value == K_NOT) &&
																	 (cur->next->tok_value == K_NULL))
													{					
														col_entry[cur_id].not_null = true;
														cur = cur->next->next;
													}
		
													if (!rc)
													{
														/* I must have either a comma or right paren */
														if ((cur->tok_value != S_RIGHT_PAREN) &&															  (cur->tok_value != S_COMMA))
														{
															rc = INVALID_COLUMN_DEFINITION;
															cur->tok_value = INVALID;
														}
														else
													  {
															if (cur->tok_value == S_RIGHT_PAREN)
															{
																column_done = true;
															}
															cur = cur->next;
														}
													}
												}
											}
										}	/* end char(n) processing */
									}
								} /* end char processing */
							}
						}  // duplicate column name
					} // invalid column name

					/* If rc=0, then get ready for the next column */
					if (!rc)
					{
						cur_id++;
					}

				} while ((rc == 0) && (!column_done));
	
				if ((column_done) && (cur->tok_value != EOC))
				{
					rc = INVALID_TABLE_DEFINITION;
					cur->tok_value = INVALID;
				}

				if (!rc)
				{
					/* Now finished building tpd and add it to the tpd list */
					tab_entry.num_columns = cur_id;
					tab_entry.tpd_size = sizeof(tpd_entry) + 
															 sizeof(cd_entry) *	tab_entry.num_columns;
				  tab_entry.cd_offset = sizeof(tpd_entry);
					new_entry = (tpd_entry*)calloc(1, tab_entry.tpd_size);

					if (new_entry == NULL)
					{
						rc = MEMORY_ERROR;
					}
					else
					{
						memcpy((void*)new_entry,
							     (void*)&tab_entry,
									 sizeof(tpd_entry));
		
						memcpy((void*)((char*)new_entry + sizeof(tpd_entry)),
									 (void*)col_entry,
									 sizeof(cd_entry) * tab_entry.num_columns);
	
						rc = add_tpd_to_list(new_entry);

						if (!rc) {
							/* Create <table>.tab using the descriptor we just built before */
							int frc = create_table_data_file(new_entry);
							if (frc) rc = frc;
						}

						/* Refresh the in-memory catalog so that future lookups work */
						if (!rc) {
							int irc = initialize_tpd_list();
							if (irc) rc = irc;
						}

						free(new_entry);
					}
				}
			}
		}
	}
  return rc;
}

int sem_drop_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;

	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if (cur->next->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->next->tok_value = INVALID;
		}
		else
		{
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}
			else
			{
				/* Found a valid tpd, drop it from tpd list */
				rc = drop_tpd_from_list(cur->tok_string);
				if (!rc) {
					int frc = drop_table_data_file(cur->tok_string);
					if (frc) rc = frc;
				}
			}
		}
	}

  return rc;
}

int sem_list_tables()
{
	int rc = 0;
	int num_tables = g_tpd_list->num_tables;
	tpd_entry *cur = &(g_tpd_list->tpd_start);

	if (num_tables == 0)
	{
		printf("\nThere are currently no tables defined\n");
	}
	else
	{
		printf("\nTable List\n");
		printf("*****************\n");
		while (num_tables-- > 0)
		{
			printf("%s\n", cur->table_name);
			if (num_tables > 0)
			{
				cur = (tpd_entry*)((char*)cur + cur->tpd_size);
			}
		}
		printf("****** End ******\n");
	}

  return rc;
}

int sem_list_schema(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	char tab_name[MAX_IDENT_LEN+1];
	char filename[MAX_IDENT_LEN+1];
	bool report = false;
	FILE *fhandle = NULL;
	int i = 0;

	cur = t_list;

	if (cur->tok_value != K_FOR)
  {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;

		if ((cur->tok_class != keyword) &&
			  (cur->tok_class != identifier) &&
				(cur->tok_class != type_name))
		{
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			memset(filename, '\0', MAX_IDENT_LEN+1);
			strcpy(tab_name, cur->tok_string);
			cur = cur->next;

			if (cur->tok_value != EOC)
			{
				if (cur->tok_value == K_TO)
				{
					cur = cur->next;
					
					if ((cur->tok_class != keyword) &&
						  (cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_REPORT_FILE_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						if (cur->next->tok_value != EOC)
						{
							rc = INVALID_STATEMENT;
							cur->next->tok_value = INVALID;
						}
						else
						{
							/* We have a valid file name */
							strcpy(filename, cur->tok_string);
							report = true;
						}
					}
				}
				else
				{ 
					/* Missing the TO keyword */
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
				}
			}

			if (!rc)
			{
				if ((tab_entry = get_tpd_from_list(tab_name)) == NULL)
				{
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
				}
				else
				{
					if (report)
					{
						if((fhandle = fopen(filename, "a+tc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
					}

					if (!rc)
					{
						/* Find correct tpd, need to parse column and index information */

						/* First, write the tpd_entry information */
						printf("Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
						printf("Table Name               (table_name)  = %s\n", tab_entry->table_name);
						printf("Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
						printf("Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
            printf("Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 

						if (report)
						{
							fprintf(fhandle, "Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
							fprintf(fhandle, "Table Name               (table_name)  = %s\n", tab_entry->table_name);
							fprintf(fhandle, "Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
							fprintf(fhandle, "Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
              fprintf(fhandle, "Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 
						}

						/* Next, write the cd_entry information */
						for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
								i < tab_entry->num_columns; i++, col_entry++)
						{
							printf("Column Name   (col_name) = %s\n", col_entry->col_name);
							printf("Column Id     (col_id)   = %d\n", col_entry->col_id);
							printf("Column Type   (col_type) = %d\n", col_entry->col_type);
							printf("Column Length (col_len)  = %d\n", col_entry->col_len);
							printf("Not Null flag (not_null) = %d\n\n", col_entry->not_null);

							if (report)
							{
								fprintf(fhandle, "Column Name   (col_name) = %s\n", col_entry->col_name);
								fprintf(fhandle, "Column Id     (col_id)   = %d\n", col_entry->col_id);
								fprintf(fhandle, "Column Type   (col_type) = %d\n", col_entry->col_type);
								fprintf(fhandle, "Column Length (col_len)  = %d\n", col_entry->col_len);
								fprintf(fhandle, "Not Null Flag (not_null) = %d\n\n", col_entry->not_null);
							}
						}
	
						if (report)
						{
							fflush(fhandle);
							fclose(fhandle);
						}
					} // File open error							
				} // Table not exist
			} // no semantic errors
		} // Invalid table name
	} // Invalid statement

  return rc;
}

int sem_insert_into(token_list *t_list)
{
	int rc = 0;
	token_list *current_token = t_list;

	// table name
	if ((current_token->tok_class != keyword) && (current_token->tok_class != identifier) && (current_token->tok_class != type_name)) {
		rc = INVALID_TABLE_NAME;
		current_token->tok_value = INVALID;
		return rc;
	}

	char table_name[MAX_IDENT_LEN+1] = {0};
	strcpy(table_name, current_token->tok_string);

	tpd_entry *table_descriptor = get_tpd_from_list(table_name);
	if (!table_descriptor) {
		rc = TABLE_NOT_EXIST;
		current_token->tok_value = INVALID;
		return rc;
	}

	current_token = current_token->next;
	if (current_token->tok_value != K_VALUES) {
		rc = INVALID_STATEMENT;
		current_token->tok_value = INVALID;
		return rc;
	}
	current_token = current_token->next;
	if (current_token->tok_value != S_LEFT_PAREN) {
		rc = INVALID_STATEMENT;
		current_token->tok_value = INVALID;
		return rc;
	}
	current_token = current_token->next;

	FILE *table_file = NULL; 
	table_file_header file_header;
	if ((rc = open_tab_rw(table_name, &table_file, &file_header))) return rc;

	if (file_header.num_records >= 100) {
		fclose(table_file);
		return MEMORY_ERROR;
	} // project cap

	// prepare one record buffer
	int record_size = file_header.record_size;
	unsigned char *row_buffer = (unsigned char*)calloc(1, record_size);
	if (!row_buffer) {
		fclose(table_file);
		return MEMORY_ERROR;
	}

	cd_entry *current_column = (cd_entry*)((char*)table_descriptor + table_descriptor->cd_offset);
	int buffer_offset = 0;

	for (int column_index = 0; column_index < table_descriptor->num_columns; ++column_index, ++current_column) {
		// expect a value: STRING_LITERAL | INT_LITERAL | K_NULL
		if ((current_token->tok_value != STRING_LITERAL) && (current_token->tok_value != INT_LITERAL) && (current_token->tok_value != K_NULL)) {
			rc = INVALID_INSERT_DEFINITION;
			current_token->tok_value = INVALID;
			break;
		}

		if (current_token->tok_value == K_NULL) {
			if (current_column->not_null) { rc = NOT_NULL_CONSTRAINT_VIOLATION; current_token->tok_value = INVALID; break; }
			row_buffer[buffer_offset++] = 0; // length=0
			// skip payload area
			buffer_offset += (current_column->col_type == T_INT) ? 4 : current_column->col_len;
		} else if (current_column->col_type == T_INT) {
			if (current_token->tok_value != INT_LITERAL) { rc = TYPE_MISMATCH; current_token->tok_value = INVALID; break; }
			long parsed_value = strtol(current_token->tok_string, NULL, 10);
			row_buffer[buffer_offset++] = 4; // length
			int32_t int_value = (int32_t)parsed_value;
			memcpy(row_buffer + buffer_offset, &int_value, 4);
			buffer_offset += 4;
		} else {
			// CHAR/VARCHAR(n) stored as fixed n with length tag
			if (current_token->tok_value != STRING_LITERAL) { rc = TYPE_MISMATCH; current_token->tok_value = INVALID; break; }
			int string_length = (int)strlen(current_token->tok_string);
			if (string_length <= 0 || string_length > current_column->col_len) { rc = INVALID_COLUMN_LENGTH; current_token->tok_value = INVALID; break; }
			row_buffer[buffer_offset++] = (unsigned char)string_length;
			memcpy(row_buffer + buffer_offset, current_token->tok_string, string_length);
			buffer_offset += current_column->col_len; // advance fixed area (rest stays zero)
		}

		current_token = current_token->next;

		// comma or right paren
		if (column_index < table_descriptor->num_columns - 1) {
			if (current_token->tok_value != S_COMMA) { rc = INVALID_INSERT_DEFINITION; current_token->tok_value = INVALID; break; }
			current_token = current_token->next;
		} else {
			if (current_token->tok_value != S_RIGHT_PAREN) { rc = INVALID_INSERT_DEFINITION; current_token->tok_value = INVALID; break; }
			current_token = current_token->next;
		}
	}

	if (!rc) {
		if (current_token->tok_value != EOC) { rc = INVALID_STATEMENT; current_token->tok_value = INVALID; }
	}

	if (!rc) {
		long write_position = row_pos(&file_header, file_header.num_records);
		fseek(table_file, write_position, SEEK_SET);
		if (fwrite(row_buffer, record_size, 1, table_file) != 1) rc = FILE_WRITE_ERROR;
		else {
			file_header.num_records += 1;
			rc = write_header(table_file, &file_header);
		}
	}

	free(row_buffer);
	fclose(table_file);
	return rc;
}

int sem_select_star(token_list *t_list)
{
	int rc = 0;
	token_list *current_token = t_list;           // points at '*'
	if (current_token->tok_value != S_STAR) {
		rc = INVALID_STATEMENT;
		current_token->tok_value = INVALID;
		return rc;
	}
	current_token = current_token->next;
	if (current_token->tok_value != K_FROM) {
		rc = INVALID_STATEMENT;
		current_token->tok_value = INVALID;
		return rc;
	}
	current_token = current_token->next;

	if ((current_token->tok_class != keyword) && (current_token->tok_class != identifier) && (current_token->tok_class != type_name)) {
		rc = INVALID_TABLE_NAME;
		current_token->tok_value = INVALID;
		return rc;
	}

	char table_name[MAX_IDENT_LEN+1] = {0};
	strcpy(table_name, current_token->tok_string);

	tpd_entry *table_descriptor = get_tpd_from_list(table_name);
	if (!table_descriptor) { rc = TABLE_NOT_EXIST; current_token->tok_value = INVALID; return rc; }

	current_token = current_token->next;
	
	// Check for NATURAL JOIN
	bool has_join = false;
	char table_name2[MAX_IDENT_LEN+1] = {0};
	tpd_entry *table_descriptor2 = NULL;
	
	if (current_token->tok_value == K_NATURAL) {
		has_join = true;
		current_token = current_token->next;
		
		if (current_token->tok_value != K_JOIN) {
			rc = INVALID_STATEMENT;
			current_token->tok_value = INVALID;
			return rc;
		}
		current_token = current_token->next;
		
		if ((current_token->tok_class != keyword) && (current_token->tok_class != identifier) && (current_token->tok_class != type_name)) {
			rc = INVALID_TABLE_NAME;
			current_token->tok_value = INVALID;
			return rc;
		}
		
		strcpy(table_name2, current_token->tok_string);
		table_descriptor2 = get_tpd_from_list(table_name2);
		if (!table_descriptor2) { rc = TABLE_NOT_EXIST; current_token->tok_value = INVALID; return rc; }
		
		current_token = current_token->next;
	}
	
	if (current_token->tok_value != EOC) {
		rc = INVALID_STATEMENT;
		current_token->tok_value = INVALID;
		return rc;
	}

	if (!has_join) {
		// Single table SELECT - original logic
		FILE *table_file = NULL; 
		table_file_header file_header;
		if ((rc = open_tab_rw(table_name, &table_file, &file_header))) return rc;

		cd_entry *column_descriptors = (cd_entry*)((char*)table_descriptor + table_descriptor->cd_offset);

		/* Compute column widths based on column definitions and names */
		int col_count = table_descriptor->num_columns;
		int widths[MAX_NUM_COL] = {0};
		for (int i = 0; i < col_count; ++i) {
			int w = (int)strlen(column_descriptors[i].col_name);
			if (column_descriptors[i].col_type == T_INT) {
				if (w < 5) w = 5;
			} else {
				if (w < column_descriptors[i].col_len) w = column_descriptors[i].col_len;
			}
			widths[i] = w;
		}

		/* Print header */
		for (int i = 0; i < col_count; ++i) {
			printf("%-*s", widths[i], column_descriptors[i].col_name);
			if (i + 1 < col_count) printf(" ");
		}
		printf("\n");
		for (int i = 0; i < col_count; ++i) {
			for (int j = 0; j < widths[i]; ++j) putchar('-');
			if (i + 1 < col_count) putchar(' ');
		}
		printf("\n");

		int record_size = file_header.record_size;
		unsigned char *row_buffer = (unsigned char*)malloc(record_size);
		if (!row_buffer) { fclose(table_file); return MEMORY_ERROR; }

		/* Count of printed records */
		int selected = 0;

		for (int row_index = 0; row_index < file_header.num_records; ++row_index) {
			fseek(table_file, row_pos(&file_header, row_index), SEEK_SET);
			if (fread(row_buffer, record_size, 1, table_file) != 1) { rc = FILE_OPEN_ERROR; break; }

			int buffer_offset = 0;
			for (int column_index = 0; column_index < col_count; column_index++) {
				unsigned char field_length = row_buffer[buffer_offset++];

				if (column_descriptors[column_index].col_type == T_INT) {
					if (field_length == 0) printf("%-*s", widths[column_index], "NULL");
					else { int32_t int_value = 0; memcpy(&int_value, row_buffer + buffer_offset, 4); printf("%*d", widths[column_index], int_value); }
					buffer_offset += 4;
				} else {
					if (field_length == 0) printf("%-*s", widths[column_index], "NULL");
					else printf("%-*.*s", widths[column_index], (int)field_length, (char*)(row_buffer + buffer_offset));
					buffer_offset += column_descriptors[column_index].col_len;
				}
				if (column_index + 1 < col_count) printf(" "); else printf("\n");
			}
			/* Count this printed record */
			selected++;
		}

		free(row_buffer);
		fclose(table_file);

		/* Print count summary similar to professor output */
		printf("\n %d record(s) selected.\n\n", selected);
		
	} else {
		// NATURAL JOIN logic
		rc = sem_select_natural_join(table_descriptor, table_descriptor2, table_name, table_name2);
	}
	
	return rc;
}

/* Perform NATURAL JOIN on two tables */
int sem_select_natural_join(tpd_entry *tpd1, tpd_entry *tpd2, const char *table_name1, const char *table_name2)
{
	int rc = 0;
	
	// Find common columns
	int common_map1[MAX_NUM_COL];
	int common_map2[MAX_NUM_COL];
	int num_common = find_common_columns(tpd1, tpd2, common_map1, common_map2);
	
	if (num_common == 0) {
		printf("Warning: No common columns found for NATURAL JOIN\n");
		return 0;
	}
	
	// Open both table files
	FILE *file1 = NULL, *file2 = NULL;
	table_file_header header1, header2;
	
	if ((rc = open_tab_rw(table_name1, &file1, &header1))) {
		return rc;
	}
	
	if ((rc = open_tab_rw(table_name2, &file2, &header2))) {
		fclose(file1);
		return rc;
	}
	
	// Allocate row buffers
	unsigned char *row_buffer1 = (unsigned char*)malloc(header1.record_size);
	unsigned char *row_buffer2 = (unsigned char*)malloc(header2.record_size);
	
	if (!row_buffer1 || !row_buffer2) {
		free(row_buffer1);
		free(row_buffer2);
		fclose(file1);
		fclose(file2);
		return MEMORY_ERROR;
	}
	
	// Print header row
	print_join_header(tpd1, tpd2, common_map1, common_map2, num_common);

	/* Keep a count of printed joined rows */
	int selected = 0;
	
	// Perform nested loop join
	cd_entry *cols1 = (cd_entry*)((char*)tpd1 + tpd1->cd_offset);
	cd_entry *cols2 = (cd_entry*)((char*)tpd2 + tpd2->cd_offset);
	
	for (int row_idx1 = 0; row_idx1 < header1.num_records; row_idx1++) {
		fseek(file1, row_pos(&header1, row_idx1), SEEK_SET);
		if (fread(row_buffer1, header1.record_size, 1, file1) != 1) {
			rc = FILE_OPEN_ERROR;
			break;
		}
		
		for (int row_idx2 = 0; row_idx2 < header2.num_records; row_idx2++) {
			fseek(file2, row_pos(&header2, row_idx2), SEEK_SET);
			if (fread(row_buffer2, header2.record_size, 1, file2) != 1) {
				rc = FILE_OPEN_ERROR;
				break;
			}
			
			// Check if rows match on common columns
				if (rows_match_on_common_columns(
				row_buffer1, row_buffer2, 
			    cols1, cols2,
			    common_map1, common_map2, num_common
			)) {
					print_joined_row(row_buffer1, row_buffer2, tpd1, tpd2, common_map1, common_map2, num_common);
					selected++;
			}
		}
		
		if (rc) break;
	}
	
	// Cleanup
	free(row_buffer1);
	free(row_buffer2);
	fclose(file1);
	fclose(file2);

	/* Print count summary for NATURAL JOIN */
	printf("\n %d record(s) selected.\n\n", selected);

	return rc;
}

int initialize_tpd_list()
{
	int rc = 0;
	FILE *fhandle = NULL;
//	struct _stat file_stat;
	struct stat file_stat;

  /* Open for read */
  if((fhandle = fopen("dbfile.bin", "rbc")) == NULL)
	{
		if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
    else
		{
			g_tpd_list = NULL;
			g_tpd_list = (tpd_list*)calloc(1, sizeof(tpd_list));
			
			if (!g_tpd_list)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				g_tpd_list->list_size = sizeof(tpd_list);
				fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);
			}
		}
	}
	else
	{
		/* There is a valid dbfile.bin file - get file size */
//		_fstat(_fileno(fhandle), &file_stat);
		fstat(fileno(fhandle), &file_stat);
		printf("dbfile.bin size = %d\n", file_stat.st_size);

		g_tpd_list = (tpd_list*)calloc(1, file_stat.st_size);

		if (!g_tpd_list)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			fread(g_tpd_list, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);

			if (g_tpd_list->list_size != file_stat.st_size)
			{
				rc = DBFILE_CORRUPTION;
			}

		}
	}
    
	return rc;
}
	
int add_tpd_to_list(tpd_entry *tpd)
{
	int rc = 0;
	int old_size = 0;
	FILE *fhandle = NULL;

	if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
  else
	{
		old_size = g_tpd_list->list_size;

		if (g_tpd_list->num_tables == 0)
		{
			/* If this is an empty list, overlap the dummy header */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += (tpd->tpd_size - sizeof(tpd_entry));
			fwrite(g_tpd_list, old_size - sizeof(tpd_entry), 1, fhandle);
		}
		else
		{
			/* There is at least 1, just append at the end */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += tpd->tpd_size;
			fwrite(g_tpd_list, old_size, 1, fhandle);
		}

		fwrite(tpd, tpd->tpd_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	return rc;
}

int drop_tpd_from_list(char *tabname)
{
	int rc = 0;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;
	int count = 0;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (strcasecmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				int old_size = 0;
				FILE *fhandle = NULL;

				if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
			  else
				{
					old_size = g_tpd_list->list_size;

					if (count == 0)
					{
						/* If this is the first entry */
						g_tpd_list->num_tables--;

						if (g_tpd_list->num_tables == 0)
						{
							/* This is the last table, null out dummy header */
							memset((void*)g_tpd_list, '\0', sizeof(tpd_list));
							g_tpd_list->list_size = sizeof(tpd_list);
							fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
						}
						else
						{
							/* First in list, but not the last one */
							g_tpd_list->list_size -= cur->tpd_size;

							/* First, write the 8 byte header */
							fwrite(g_tpd_list, sizeof(tpd_list) - sizeof(tpd_entry),
								     1, fhandle);

							/* Now write everything starting after the cur entry */
							fwrite((char*)cur + cur->tpd_size,
								     old_size - cur->tpd_size -
										 (sizeof(tpd_list) - sizeof(tpd_entry)),
								     1, fhandle);
						}
					}
					else
					{
						/* This is NOT the first entry - count > 0 */
						g_tpd_list->num_tables--;
					 	g_tpd_list->list_size -= cur->tpd_size;

						/* First, write everything from beginning to cur */
						fwrite(g_tpd_list, ((char*)cur - (char*)g_tpd_list),
									 1, fhandle);

						/* Check if cur is the last entry. Note that g_tdp_list->list_size
						   has already subtracted the cur->tpd_size, therefore it will
						   point to the start of cur if cur was the last entry */
						if ((char*)g_tpd_list + g_tpd_list->list_size == (char*)cur)
						{
							/* If true, nothing else to write */
						}
						else
						{
							/* NOT the last entry, copy everything from the beginning of the
							   next entry which is (cur + cur->tpd_size) and the remaining size */
							fwrite((char*)cur + cur->tpd_size,
										 old_size - cur->tpd_size -
										 ((char*)cur - (char*)g_tpd_list),							     
								     1, fhandle);
						}
					}

					fflush(fhandle);
					fclose(fhandle);
				}

				
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
					count++;
				}
			}
		}
	}
	
	if (!found)
	{
		rc = INVALID_TABLE_NAME;
	}

	return rc;
}

tpd_entry* get_tpd_from_list(char *tabname)
{
	tpd_entry *tpd = NULL;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (strcasecmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				tpd = cur;
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
				}
			}
		}
	}

	return tpd;
}