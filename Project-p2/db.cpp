/************************************************************
        Project#1:	CLP & DDL
 ************************************************************/

#include "db.h"
#include <ctype.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#if defined(_WIN32) || defined(_WIN64)
#define strcasecmp _stricmp
#endif

/* Globals to store output column ordering/widths for JOIN printing */
static int join_out_count = 0;
static int join_out_widths[MAX_NUM_COL * 2];
static int join_out_types[MAX_NUM_COL * 2];
static char join_out_names[MAX_NUM_COL * 2][MAX_IDENT_LEN + 8];

static int open_tab_rw(const char *table_name, FILE **pf,
                       table_file_header *hdr) {
  char fname[MAX_IDENT_LEN + 5] = {0};
  snprintf(fname, sizeof(fname), "%s.tab", table_name);

  *pf = fopen(fname, "rb+"); // read/write binary
  if (!*pf)
    return FILE_OPEN_ERROR;

  fseek(*pf, 0, SEEK_SET);
  if (fread(hdr, sizeof(*hdr), 1, *pf) != 1) {
    fclose(*pf);
    *pf = NULL;
    return FILE_OPEN_ERROR;
  }
  return 0;
}

static int write_header(FILE *f, const table_file_header *hdr_in) {
  table_file_header on_disk = *hdr_in;
  on_disk.tpd_ptr = 0; // zero when writing

  /* Recompute on-disk file_size to reflect actual number of records */
  on_disk.file_size =
      on_disk.record_offset + on_disk.record_size * on_disk.num_records;

  fseek(f, 0, SEEK_SET);
  if (fwrite(&on_disk, sizeof(on_disk), 1, f) != 1)
    return FILE_WRITE_ERROR;
  fflush(f);
  return 0;
}

static long row_pos(const table_file_header *hdr, int row_idx) {
  return (long)hdr->record_offset + (long)row_idx * (long)hdr->record_size;
}

static inline int round4(int n) { return (n + 3) & ~3; }

static int compute_record_size_from_tpd(const tpd_entry *tpd) {
  int rec = 0;
  const cd_entry *col = (const cd_entry *)((const char *)tpd + tpd->cd_offset);
  for (int i = 0; i < tpd->num_columns; ++i, ++col) {
    if (col->col_type == T_INT)
      rec += 1 + 4; // 1-byte length + 4
    else
      rec += 1 + col->col_len; // CHAR/VARCHAR(n)
  }
  return round4(rec);
}

static int create_table_data_file(const tpd_entry *tpd) {
  char fname[MAX_IDENT_LEN + 5] = {0};
  snprintf(fname, sizeof(fname), "%s.tab", tpd->table_name);

  int rec_size = compute_record_size_from_tpd(tpd);

  table_file_header hdr = {0};
  hdr.record_size = rec_size;
  hdr.num_records = 0;
  hdr.record_offset = sizeof(table_file_header);
  /* Initially only write the header; do not pre-allocate space for MAX_ROWS */
  hdr.file_size = hdr.record_offset;
  hdr.file_header_flag = 0;
  hdr.tpd_ptr = 0; // zero on disk

  /* Write only the header to create a small initial file. File will grow as
   * records are inserted. */
  FILE *fh = fopen(fname, "wb");
  if (!fh)
    return FILE_OPEN_ERROR;
  if (fwrite(&hdr, sizeof(hdr), 1, fh) != 1) {
    fclose(fh);
    return FILE_WRITE_ERROR;
  }
  fflush(fh);
  fclose(fh);
  return 0;
}

static int drop_table_data_file(const char *table_name) {
  char fname[MAX_IDENT_LEN + 5] = {0};
  snprintf(fname, sizeof(fname), "%s.tab", table_name);
  if (remove(fname) == 0)
    return 0;
  if (errno == ENOENT)
    return 0;
  return FILE_OPEN_ERROR;
}

/* Extract a field value from a row buffer at the specified column index */
static void extract_field_at_column(unsigned char *row_buffer,
                                    cd_entry *columns, int col_index,
                                    unsigned char *str_value, int *int_value,
                                    unsigned char *length) {
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
static bool are_fields_equal(cd_entry *col, unsigned char len1, void *val1,
                             unsigned char len2, void *val2) {
  // Both NULL
  if (len1 == 0 && len2 == 0)
    return true;

  // One NULL, one not
  if (len1 == 0 || len2 == 0)
    return false;

  // Compare based on type
  if (col->col_type == T_INT) {
    return *(int32_t *)val1 == *(int32_t *)val2;
  } else {
    return (len1 == len2) && (memcmp(val1, val2, len1) == 0);
  }
}

/**
 * Print a single field value
 */
static void print_field(cd_entry *col, unsigned char length, void *value) {
  if (length == 0) {
    printf("NULL");
  } else if (col->col_type == T_INT) {
    printf("%d", *(int32_t *)value);
  } else {
    /* Print strings without surrounding quotes */
    printf("%.*s", (int)length, (char *)value);
  }
}

/**
 * Find common columns between two tables for NATURAL JOIN
 */
static int find_common_columns(tpd_entry *tpd1, tpd_entry *tpd2, int *col_map1,
                               int *col_map2) {
  cd_entry *cols1 = (cd_entry *)((char *)tpd1 + tpd1->cd_offset);
  cd_entry *cols2 = (cd_entry *)((char *)tpd2 + tpd2->cd_offset);
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
static void print_join_header(tpd_entry *tpd1, tpd_entry *tpd2,
                              int *common_map1, int *common_map2,
                              int num_common) {
  cd_entry *cols1 = (cd_entry *)((char *)tpd1 + tpd1->cd_offset);
  cd_entry *cols2 = (cd_entry *)((char *)tpd2 + tpd2->cd_offset);
  bool first = true;

  /* We'll build an ordered list of output columns and compute widths */
  int out_col_count = 0;
  static int out_col_widths[MAX_NUM_COL * 2];
  static int out_col_types[MAX_NUM_COL * 2];
  static char out_col_names[MAX_NUM_COL * 2][MAX_IDENT_LEN + 8];

  // helper to add a column to output list
  auto add_out_col = [&](const char *name, int col_type, int col_len) {
    strncpy(out_col_names[out_col_count], name, sizeof(out_col_names[0]) - 1);
    out_col_names[out_col_count][sizeof(out_col_names[0]) - 1] = '\0';
    out_col_types[out_col_count] = col_type;
    int w = (int)strlen(name);
    if (col_type == T_INT) {
      if (w < 5)
        w = 5; // width for ints
    } else {
      if (w < col_len)
        w = col_len;
    }
    out_col_widths[out_col_count] = w;
    out_col_count++;
  };

  // Print common columns first (and add to out list)
  for (int i = 0; i < num_common; i++) {
    add_out_col(cols1[common_map1[i]].col_name, cols1[common_map1[i]].col_type,
                cols1[common_map1[i]].col_len);
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
    if (i + 1 < out_col_count)
      printf(" ");
  }
  printf("\n");

  /* Print separator line */
  for (int i = 0; i < out_col_count; ++i) {
    for (int j = 0; j < out_col_widths[i]; ++j)
      putchar('-');
    if (i + 1 < out_col_count)
      putchar(' ');
  }
  printf("\n");

  /* Store generated widths and counts in static globals for use by
   * print_joined_row */
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
static bool rows_match_on_common_columns(unsigned char *row1,
                                         unsigned char *row2, cd_entry *cols1,
                                         cd_entry *cols2, int *common_map1,
                                         int *common_map2, int num_common) {
  for (int c = 0; c < num_common; c++) {
    int idx1 = common_map1[c];
    int idx2 = common_map2[c];

    unsigned char len1, len2;
    unsigned char str_val1[256] = {0}, str_val2[256] = {0};
    int int_val1 = 0, int_val2 = 0;

    extract_field_at_column(row1, cols1, idx1, str_val1, &int_val1, &len1);
    extract_field_at_column(row2, cols2, idx2, str_val2, &int_val2, &len2);

    void *v1 =
        (cols1[idx1].col_type == T_INT) ? (void *)&int_val1 : (void *)str_val1;
    void *v2 =
        (cols2[idx2].col_type == T_INT) ? (void *)&int_val2 : (void *)str_val2;

    if (!are_fields_equal(&cols1[idx1], len1, v1, len2, v2)) {
      return false;
    }
  }

  return true;
}

/**
 * Print a joined row with proper column ordering
 */
static void print_joined_row(unsigned char *row1, unsigned char *row2,
                             tpd_entry *tpd1, tpd_entry *tpd2, int *common_map1,
                             int *common_map2, int num_common) {
  cd_entry *cols1 = (cd_entry *)((char *)tpd1 + tpd1->cd_offset);
  cd_entry *cols2 = (cd_entry *)((char *)tpd2 + tpd2->cd_offset);
  int pos = 0;

  /* Print common columns (from table1) */
  for (int c = 0; c < num_common; c++) {
    int idx = common_map1[c];
    unsigned char len;
    unsigned char str_val[256] = {0};
    int int_val = 0;
    extract_field_at_column(row1, cols1, idx, str_val, &int_val, &len);

    if (join_out_types[pos] == T_INT) {
      if (len == 0)
        printf("%-*s", join_out_widths[pos], "NULL");
      else
        printf("%*d", join_out_widths[pos], int_val);
    } else {
      if (len == 0)
        printf("%-*s", join_out_widths[pos], "NULL");
      else
        printf("%-*.*s", join_out_widths[pos], (int)len, (char *)str_val);
    }
    if (pos + 1 < join_out_count)
      printf(" ");
    pos++;
  }

  /* Remaining columns from table1 */
  for (int i = 0; i < tpd1->num_columns; i++) {
    bool is_common = false;
    for (int c = 0; c < num_common; c++) {
      if (common_map1[c] == i) {
        is_common = true;
        break;
      }
    }
    if (!is_common) {
      unsigned char len;
      unsigned char str_val[256] = {0};
      int int_val = 0;
      extract_field_at_column(row1, cols1, i, str_val, &int_val, &len);

      if (join_out_types[pos] == T_INT) {
        if (len == 0)
          printf("%-*s", join_out_widths[pos], "NULL");
        else
          printf("%*d", join_out_widths[pos], int_val);
      } else {
        if (len == 0)
          printf("%-*s", join_out_widths[pos], "NULL");
        else
          printf("%-*.*s", join_out_widths[pos], (int)len, (char *)str_val);
      }
      if (pos + 1 < join_out_count)
        printf(" ");
      pos++;
    }
  }

  /* Remaining columns from table2 */
  for (int i = 0; i < tpd2->num_columns; i++) {
    bool is_common = false;
    for (int c = 0; c < num_common; c++) {
      if (common_map2[c] == i) {
        is_common = true;
        break;
      }
    }
    if (!is_common) {
      unsigned char len;
      unsigned char str_val[256] = {0};
      int int_val = 0;
      extract_field_at_column(row2, cols2, i, str_val, &int_val, &len);

      if (join_out_types[pos] == T_INT) {
        if (len == 0)
          printf("%-*s", join_out_widths[pos], "NULL");
        else
          printf("%*d", join_out_widths[pos], int_val);
      } else {
        if (len == 0)
          printf("%-*s", join_out_widths[pos], "NULL");
        else
          printf("%-*.*s", join_out_widths[pos], (int)len, (char *)str_val);
      }
      if (pos + 1 < join_out_count)
        printf(" ");
      pos++;
    }
  }

  printf("\n");
}

int main(int argc, char **argv) {
  int rc = 0;
  token_list *tok_list = NULL, *tok_ptr = NULL, *tmp_tok_ptr = NULL;

  if ((argc != 2) || (strlen(argv[1]) == 0)) {
    printf("Usage: db \"command statement\"\n");
    return 1;
  }

  rc = initialize_tpd_list();

  if (rc) {
    printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
  } else {
    rc = get_token(argv[1], &tok_list);

    /* Test code */
    tok_ptr = tok_list;
    while (tok_ptr != NULL) {
      printf("%16s \t%d \t %d\n", tok_ptr->tok_string, tok_ptr->tok_class,
             tok_ptr->tok_value);
      tok_ptr = tok_ptr->next;
    }

    if (!rc) {
      rc = do_semantic(tok_list);
    }

    if (rc) {
      bool found_error = false;
      tok_ptr = tok_list;
      while (tok_ptr != NULL) {
        if ((tok_ptr->tok_class == error) || (tok_ptr->tok_value == INVALID)) {
          printf("\nError in the string: %s\n", tok_ptr->tok_string);
          printf("rc=%d\n", rc);
          found_error = true;
          break;
        }
        tok_ptr = tok_ptr->next;
      }
      if (!found_error) {
        printf("\nError: rc=%d\n", rc);
      }
    }

    /* Whether the token list is valid or not, we need to free the memory */
    tok_ptr = tok_list;
    while (tok_ptr != NULL) {
      tmp_tok_ptr = tok_ptr->next;
      free(tok_ptr);
      tok_ptr = tmp_tok_ptr;
    }
  }

  return rc;
}

/*************************************************************
        This is a lexical analyzer for simple SQL statements
 *************************************************************/
int get_token(char *command, token_list **tok_list) {
  int rc = 0, i, j;
  char *start, *cur, temp_string[MAX_TOK_LEN];
  bool done = false;

  start = cur = command;
  while (!done) {
    bool found_keyword = false;

    /* This is the TOP Level for each token */
    memset((void *)temp_string, '\0', MAX_TOK_LEN);
    i = 0;

    /* Get rid of all the leading blanks */
    while (*cur == ' ')
      cur++;

    if (cur && isalpha(*cur)) {
      // find valid identifier
      int t_class;
      do {
        temp_string[i++] = *cur++;
      } while ((isalnum(*cur)) || (*cur == '_'));

      if (!(strchr(STRING_BREAK, *cur))) {
        /* If the next char following the keyword or identifier
           is not a blank, (, ), or a comma, then append this
                 character to temp_string, and flag this as an error */
        temp_string[i++] = *cur++;
        add_to_list(tok_list, temp_string, error, INVALID);
        rc = INVALID;
        done = true;
      } else {

        // We have an identifier with at least 1 character
        // Now check if this ident is a keyword
        for (j = 0, found_keyword = false; j < TOTAL_KEYWORDS_PLUS_TYPE_NAMES;
             j++) {
          if ((strcasecmp(keyword_table[j], temp_string) == 0)) {
            found_keyword = true;
            break;
          }
        }

        if (found_keyword) {
          if (KEYWORD_OFFSET + j < K_CREATE)
            t_class = type_name;
          else if (KEYWORD_OFFSET + j >= F_SUM)
            t_class = function_name;
          else
            t_class = keyword;

          add_to_list(tok_list, temp_string, t_class, KEYWORD_OFFSET + j);
        } else {
          if (strlen(temp_string) <= MAX_IDENT_LEN)
            add_to_list(tok_list, temp_string, identifier, IDENT);
          else {
            add_to_list(tok_list, temp_string, error, INVALID);
            rc = INVALID;
            done = true;
          }
        }

        if (!*cur) {
          add_to_list(tok_list, "", terminator, EOC);
          done = true;
        }
      }
    } else if (isdigit(*cur)) {
      // find valid number
      do {
        temp_string[i++] = *cur++;
      } while (isdigit(*cur));

      if (!(strchr(NUMBER_BREAK, *cur))) {
        /* If the next char following the keyword or identifier
           is not a blank or a ), then append this
                 character to temp_string, and flag this as an error */
        temp_string[i++] = *cur++;
        add_to_list(tok_list, temp_string, error, INVALID);
        rc = INVALID;
        done = true;
      } else {
        add_to_list(tok_list, temp_string, constant, INT_LITERAL);

        if (!*cur) {
          add_to_list(tok_list, "", terminator, EOC);
          done = true;
        }
      }
    } else if ((*cur == '(') || (*cur == ')') || (*cur == ',') ||
               (*cur == '*') || (*cur == '=') || (*cur == '<') ||
               (*cur == '>')) {
      /* Catch all the symbols here. Note: no look ahead here. */
      int t_value;
      switch (*cur) {
      case '(':
        t_value = S_LEFT_PAREN;
        break;
      case ')':
        t_value = S_RIGHT_PAREN;
        break;
      case ',':
        t_value = S_COMMA;
        break;
      case '*':
        t_value = S_STAR;
        break;
      case '=':
        t_value = S_EQUAL;
        break;
      case '<':
        t_value = S_LESS;
        break;
      case '>':
        t_value = S_GREATER;
        break;
      }

      temp_string[i++] = *cur++;

      add_to_list(tok_list, temp_string, symbol, t_value);

      if (!*cur) {
        add_to_list(tok_list, "", terminator, EOC);
        done = true;
      }
    } else if (*cur == '\'') {
      /* Find STRING_LITERRAL */
      int t_class;
      cur++;
      do {
        temp_string[i++] = *cur++;
      } while ((*cur) && (*cur != '\''));

      temp_string[i] = '\0';

      if (!*cur) {
        /* If we reach the end of line */
        add_to_list(tok_list, temp_string, error, INVALID);
        rc = INVALID;
        done = true;
      } else /* must be a ' */
      {
        add_to_list(tok_list, temp_string, constant, STRING_LITERAL);
        cur++;
        if (!*cur) {
          add_to_list(tok_list, "", terminator, EOC);
          done = true;
        }
      }
    } else {
      if (!*cur) {
        add_to_list(tok_list, "", terminator, EOC);
        done = true;
      } else {
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

void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value) {
  token_list *cur = *tok_list;
  token_list *ptr = NULL;

  // printf("%16s \t%d \t %d\n",tmp, t_class, t_value);

  ptr = (token_list *)calloc(1, sizeof(token_list));
  strcpy(ptr->tok_string, tmp);
  ptr->tok_class = t_class;
  ptr->tok_value = t_value;
  ptr->next = NULL;

  if (cur == NULL)
    *tok_list = ptr;
  else {
    while (cur->next != NULL)
      cur = cur->next;

    cur->next = ptr;
  }
  return;
}

int do_semantic(token_list *tok_list) {
  int rc = 0, cur_cmd = INVALID_STATEMENT;
  bool unique = false;
  token_list *cur = tok_list;

  if ((cur->tok_value == K_CREATE) &&
      ((cur->next != NULL) && (cur->next->tok_value == K_TABLE))) {
    printf("CREATE TABLE statement\n");
    cur_cmd = CREATE_TABLE;
    cur = cur->next->next;
  } else if ((cur->tok_value == K_DROP) &&
             ((cur->next != NULL) && (cur->next->tok_value == K_TABLE))) {
    printf("DROP TABLE statement\n");
    cur_cmd = DROP_TABLE;
    cur = cur->next->next;
  } else if ((cur->tok_value == K_LIST) &&
             ((cur->next != NULL) && (cur->next->tok_value == K_TABLE))) {
    printf("LIST TABLE statement\n");
    cur_cmd = LIST_TABLE;
    cur = cur->next->next;
  } else if ((cur->tok_value == K_LIST) &&
             ((cur->next != NULL) && (cur->next->tok_value == K_SCHEMA))) {
    printf("LIST SCHEMA statement\n");
    cur_cmd = LIST_SCHEMA;
    cur = cur->next->next;
  } else if ((cur->tok_value == K_INSERT) && (cur->next != NULL) &&
             (cur->next->tok_value == K_INTO)) {
    printf("INSERT statement\n");
    cur_cmd = INSERT;      // uses your enum (104)
    cur = cur->next->next; // point at <table_name>
  } else if ((cur->tok_value == K_DELETE) && (cur->next != NULL) &&
             (cur->next->tok_value == K_FROM)) {
    printf("DELETE statement\n");
    cur_cmd = DELETE;
    cur = cur->next->next;
  } else if ((cur->tok_value == K_UPDATE) && (cur->next != NULL)) {
    printf("UPDATE statement\n");
    cur_cmd = UPDATE;
    cur = cur->next;
  } else if (cur->tok_value == K_SELECT) {
    printf("SELECT statement\n");
    cur_cmd = SELECT;
    cur = cur->next;
  } else {
    printf("Invalid statement\n");
    rc = cur_cmd;
  }

  if (cur_cmd != INVALID_STATEMENT) {
    switch (cur_cmd) {
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
    case DELETE:
      rc = sem_delete(cur);
      break;
    case UPDATE:
      rc = sem_update(cur);
      break;
    case SELECT:
      rc = sem_select(cur);
      break;
    default:; /* no action */
    }
  }

  return rc;
}

int sem_create_table(token_list *t_list) {
  int rc = 0;
  token_list *cur;
  tpd_entry tab_entry;
  tpd_entry *new_entry = NULL;
  bool column_done = false;
  int cur_id = 0;
  cd_entry col_entry[MAX_NUM_COL];

  memset(&tab_entry, '\0', sizeof(tpd_entry));
  cur = t_list;
  if ((cur->tok_class != keyword) && (cur->tok_class != identifier) &&
      (cur->tok_class != type_name)) {
    // Error
    rc = INVALID_TABLE_NAME;
    cur->tok_value = INVALID;
  } else {
    if ((new_entry = get_tpd_from_list(cur->tok_string)) != NULL) {
      rc = DUPLICATE_TABLE_NAME;
      cur->tok_value = INVALID;
    } else {
      strcpy(tab_entry.table_name, cur->tok_string);
      cur = cur->next;
      if (cur->tok_value != S_LEFT_PAREN) {
        // Error
        rc = INVALID_TABLE_DEFINITION;
        cur->tok_value = INVALID;
      } else {
        memset(&col_entry, '\0', (MAX_NUM_COL * sizeof(cd_entry)));

        /* Now build a set of column entries */
        cur = cur->next;
        do {
          if ((cur->tok_class != keyword) && (cur->tok_class != identifier) &&
              (cur->tok_class != type_name)) {
            // Error
            rc = INVALID_COLUMN_NAME;
            cur->tok_value = INVALID;
          } else {
            int i;
            for (i = 0; i < cur_id; i++) {
              /* make column name case sensitive */
              if (strcmp(col_entry[i].col_name, cur->tok_string) == 0) {
                rc = DUPLICATE_COLUMN_NAME;
                cur->tok_value = INVALID;
                break;
              }
            }

            if (!rc) {
              strcpy(col_entry[cur_id].col_name, cur->tok_string);
              col_entry[cur_id].col_id = cur_id;
              col_entry[cur_id].not_null = false; /* set default */

              cur = cur->next;
              if (cur->tok_class != type_name) {
                // Error
                rc = INVALID_TYPE_NAME;
                cur->tok_value = INVALID;
              } else {
                /* Set the column type here, int or char */
                col_entry[cur_id].col_type = cur->tok_value;
                cur = cur->next;

                if (col_entry[cur_id].col_type == T_INT) {
                  if ((cur->tok_value != S_COMMA) &&
                      (cur->tok_value != K_NOT) &&
                      (cur->tok_value != S_RIGHT_PAREN)) {
                    rc = INVALID_COLUMN_DEFINITION;
                    cur->tok_value = INVALID;
                  } else {
                    col_entry[cur_id].col_len = sizeof(int);

                    if ((cur->tok_value == K_NOT) &&
                        (cur->next->tok_value != K_NULL)) {
                      rc = INVALID_COLUMN_DEFINITION;
                      cur->tok_value = INVALID;
                    } else if ((cur->tok_value == K_NOT) &&
                               (cur->next->tok_value == K_NULL)) {
                      col_entry[cur_id].not_null = true;
                      cur = cur->next->next;
                    }

                    if (!rc) {
                      /* I must have either a comma or right paren */
                      if ((cur->tok_value != S_RIGHT_PAREN) &&
                          (cur->tok_value != S_COMMA)) {
                        rc = INVALID_COLUMN_DEFINITION;
                        cur->tok_value = INVALID;
                      } else {
                        if (cur->tok_value == S_RIGHT_PAREN) {
                          column_done = true;
                        }
                        cur = cur->next;
                      }
                    }
                  }
                } // end of T_INT processing
                else {
                  // It must be char() or varchar()
                  if (cur->tok_value != S_LEFT_PAREN) {
                    rc = INVALID_COLUMN_DEFINITION;
                    cur->tok_value = INVALID;
                  } else {
                    /* Enter char(n) processing */
                    cur = cur->next;

                    if (cur->tok_value != INT_LITERAL) {
                      rc = INVALID_COLUMN_LENGTH;
                      cur->tok_value = INVALID;
                    } else {
                      /* Got a valid integer - convert */
                      col_entry[cur_id].col_len = atoi(cur->tok_string);
                      cur = cur->next;

                      if (cur->tok_value != S_RIGHT_PAREN) {
                        rc = INVALID_COLUMN_DEFINITION;
                        cur->tok_value = INVALID;
                      } else {
                        cur = cur->next;

                        if ((cur->tok_value != S_COMMA) &&
                            (cur->tok_value != K_NOT) &&
                            (cur->tok_value != S_RIGHT_PAREN)) {
                          rc = INVALID_COLUMN_DEFINITION;
                          cur->tok_value = INVALID;
                        } else {
                          if ((cur->tok_value == K_NOT) &&
                              (cur->next->tok_value != K_NULL)) {
                            rc = INVALID_COLUMN_DEFINITION;
                            cur->tok_value = INVALID;
                          } else if ((cur->tok_value == K_NOT) &&
                                     (cur->next->tok_value == K_NULL)) {
                            col_entry[cur_id].not_null = true;
                            cur = cur->next->next;
                          }

                          if (!rc) {
                            /* I must have either a comma or right paren */
                            if ((cur->tok_value != S_RIGHT_PAREN) &&
                                (cur->tok_value != S_COMMA)) {
                              rc = INVALID_COLUMN_DEFINITION;
                              cur->tok_value = INVALID;
                            } else {
                              if (cur->tok_value == S_RIGHT_PAREN) {
                                column_done = true;
                              }
                              cur = cur->next;
                            }
                          }
                        }
                      }
                    } /* end char(n) processing */
                  }
                } /* end char processing */
              }
            } // duplicate column name
          } // invalid column name

          /* If rc=0, then get ready for the next column */
          if (!rc) {
            cur_id++;
          }

        } while ((rc == 0) && (!column_done));

        if ((column_done) && (cur->tok_value != EOC)) {
          rc = INVALID_TABLE_DEFINITION;
          cur->tok_value = INVALID;
        }

        if (!rc) {
          /* Now finished building tpd and add it to the tpd list */
          tab_entry.num_columns = cur_id;
          tab_entry.tpd_size =
              sizeof(tpd_entry) + sizeof(cd_entry) * tab_entry.num_columns;
          tab_entry.cd_offset = sizeof(tpd_entry);
          new_entry = (tpd_entry *)calloc(1, tab_entry.tpd_size);

          if (new_entry == NULL) {
            rc = MEMORY_ERROR;
          } else {
            memcpy((void *)new_entry, (void *)&tab_entry, sizeof(tpd_entry));

            memcpy((void *)((char *)new_entry + sizeof(tpd_entry)),
                   (void *)col_entry, sizeof(cd_entry) * tab_entry.num_columns);

            rc = add_tpd_to_list(new_entry);

            if (!rc) {
              /* Create <table>.tab using the descriptor we just built before */
              int frc = create_table_data_file(new_entry);
              if (frc)
                rc = frc;
            }

            /* Refresh the in-memory catalog so that future lookups work */
            if (!rc) {
              int irc = initialize_tpd_list();
              if (irc)
                rc = irc;
            }

            free(new_entry);
          }
        }
      }
    }
  }
  return rc;
}

int sem_drop_table(token_list *t_list) {
  int rc = 0;
  token_list *cur;
  tpd_entry *tab_entry = NULL;

  cur = t_list;
  if ((cur->tok_class != keyword) && (cur->tok_class != identifier) &&
      (cur->tok_class != type_name)) {
    // Error
    rc = INVALID_TABLE_NAME;
    cur->tok_value = INVALID;
  } else {
    if (cur->next->tok_value != EOC) {
      rc = INVALID_STATEMENT;
      cur->next->tok_value = INVALID;
    } else {
      if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL) {
        rc = TABLE_NOT_EXIST;
        cur->tok_value = INVALID;
      } else {
        /* Found a valid tpd, drop it from tpd list */
        rc = drop_tpd_from_list(cur->tok_string);
        if (!rc) {
          int frc = drop_table_data_file(cur->tok_string);
          if (frc)
            rc = frc;
        }
      }
    }
  }

  return rc;
}

int sem_list_tables() {
  int rc = 0;
  int num_tables = g_tpd_list->num_tables;
  tpd_entry *cur = &(g_tpd_list->tpd_start);

  if (num_tables == 0) {
    printf("\nThere are currently no tables defined\n");
  } else {
    printf("\nTable List\n");
    printf("*****************\n");
    while (num_tables-- > 0) {
      printf("%s\n", cur->table_name);
      if (num_tables > 0) {
        cur = (tpd_entry *)((char *)cur + cur->tpd_size);
      }
    }
    printf("****** End ******\n");
  }

  return rc;
}

int sem_list_schema(token_list *t_list) {
  int rc = 0;
  token_list *cur;
  tpd_entry *tab_entry = NULL;
  cd_entry *col_entry = NULL;
  char tab_name[MAX_IDENT_LEN + 1];
  char filename[MAX_IDENT_LEN + 1];
  bool report = false;
  FILE *fhandle = NULL;
  int i = 0;

  cur = t_list;

  if (cur->tok_value != K_FOR) {
    rc = INVALID_STATEMENT;
    cur->tok_value = INVALID;
  } else {
    cur = cur->next;

    if ((cur->tok_class != keyword) && (cur->tok_class != identifier) &&
        (cur->tok_class != type_name)) {
      // Error
      rc = INVALID_TABLE_NAME;
      cur->tok_value = INVALID;
    } else {
      memset(filename, '\0', MAX_IDENT_LEN + 1);
      strcpy(tab_name, cur->tok_string);
      cur = cur->next;

      if (cur->tok_value != EOC) {
        if (cur->tok_value == K_TO) {
          cur = cur->next;

          if ((cur->tok_class != keyword) && (cur->tok_class != identifier) &&
              (cur->tok_class != type_name)) {
            // Error
            rc = INVALID_REPORT_FILE_NAME;
            cur->tok_value = INVALID;
          } else {
            if (cur->next->tok_value != EOC) {
              rc = INVALID_STATEMENT;
              cur->next->tok_value = INVALID;
            } else {
              /* We have a valid file name */
              strcpy(filename, cur->tok_string);
              report = true;
            }
          }
        } else {
          /* Missing the TO keyword */
          rc = INVALID_STATEMENT;
          cur->tok_value = INVALID;
        }
      }

      if (!rc) {
        if ((tab_entry = get_tpd_from_list(tab_name)) == NULL) {
          rc = TABLE_NOT_EXIST;
          cur->tok_value = INVALID;
        } else {
          if (report) {
            if ((fhandle = fopen(filename, "a+tc")) == NULL) {
              rc = FILE_OPEN_ERROR;
            }
          }

          if (!rc) {
            /* Find correct tpd, need to parse column and index information */

            /* First, write the tpd_entry information */
            printf("Table PD size            (tpd_size)    = %d\n",
                   tab_entry->tpd_size);
            printf("Table Name               (table_name)  = %s\n",
                   tab_entry->table_name);
            printf("Number of Columns        (num_columns) = %d\n",
                   tab_entry->num_columns);
            printf("Column Descriptor Offset (cd_offset)   = %d\n",
                   tab_entry->cd_offset);
            printf("Table PD Flags           (tpd_flags)   = %d\n\n",
                   tab_entry->tpd_flags);

            if (report) {
              fprintf(fhandle, "Table PD size            (tpd_size)    = %d\n",
                      tab_entry->tpd_size);
              fprintf(fhandle, "Table Name               (table_name)  = %s\n",
                      tab_entry->table_name);
              fprintf(fhandle, "Number of Columns        (num_columns) = %d\n",
                      tab_entry->num_columns);
              fprintf(fhandle, "Column Descriptor Offset (cd_offset)   = %d\n",
                      tab_entry->cd_offset);
              fprintf(fhandle,
                      "Table PD Flags           (tpd_flags)   = %d\n\n",
                      tab_entry->tpd_flags);
            }

            /* Next, write the cd_entry information */
            for (i = 0, col_entry = (cd_entry *)((char *)tab_entry +
                                                 tab_entry->cd_offset);
                 i < tab_entry->num_columns; i++, col_entry++) {
              printf("Column Name   (col_name) = %s\n", col_entry->col_name);
              printf("Column Id     (col_id)   = %d\n", col_entry->col_id);
              printf("Column Type   (col_type) = %d\n", col_entry->col_type);
              printf("Column Length (col_len)  = %d\n", col_entry->col_len);
              printf("Not Null flag (not_null) = %d\n\n", col_entry->not_null);

              if (report) {
                fprintf(fhandle, "Column Name   (col_name) = %s\n",
                        col_entry->col_name);
                fprintf(fhandle, "Column Id     (col_id)   = %d\n",
                        col_entry->col_id);
                fprintf(fhandle, "Column Type   (col_type) = %d\n",
                        col_entry->col_type);
                fprintf(fhandle, "Column Length (col_len)  = %d\n",
                        col_entry->col_len);
                fprintf(fhandle, "Not Null Flag (not_null) = %d\n\n",
                        col_entry->not_null);
              }
            }

            if (report) {
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

int sem_insert_into(token_list *t_list) {
  int rc = 0;
  token_list *current_token = t_list;

  // table name
  if ((current_token->tok_class != keyword) &&
      (current_token->tok_class != identifier) &&
      (current_token->tok_class != type_name)) {
    rc = INVALID_TABLE_NAME;
    current_token->tok_value = INVALID;
    return rc;
  }

  char table_name[MAX_IDENT_LEN + 1] = {0};
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
  if ((rc = open_tab_rw(table_name, &table_file, &file_header)))
    return rc;

  if (file_header.num_records >= 100) {
    fclose(table_file);
    return MEMORY_ERROR;
  } // project cap

  // prepare one record buffer
  int record_size = file_header.record_size;
  unsigned char *row_buffer = (unsigned char *)calloc(1, record_size);
  if (!row_buffer) {
    fclose(table_file);
    return MEMORY_ERROR;
  }

  cd_entry *current_column =
      (cd_entry *)((char *)table_descriptor + table_descriptor->cd_offset);
  int buffer_offset = 0;

  for (int column_index = 0; column_index < table_descriptor->num_columns;
       ++column_index, ++current_column) {
    // expect a value: STRING_LITERAL | INT_LITERAL | K_NULL
    if ((current_token->tok_value != STRING_LITERAL) &&
        (current_token->tok_value != INT_LITERAL) &&
        (current_token->tok_value != K_NULL)) {
      rc = INVALID_INSERT_DEFINITION;
      current_token->tok_value = INVALID;
      break;
    }

    if (current_token->tok_value == K_NULL) {
      if (current_column->not_null) {
        rc = NOT_NULL_CONSTRAINT_VIOLATION;
        current_token->tok_value = INVALID;
        break;
      }
      row_buffer[buffer_offset++] = 0; // length=0
      // skip payload area
      buffer_offset +=
          (current_column->col_type == T_INT) ? 4 : current_column->col_len;
    } else if (current_column->col_type == T_INT) {
      if (current_token->tok_value != INT_LITERAL) {
        rc = TYPE_MISMATCH;
        current_token->tok_value = INVALID;
        break;
      }
      long parsed_value = strtol(current_token->tok_string, NULL, 10);
      row_buffer[buffer_offset++] = 4; // length
      int32_t int_value = (int32_t)parsed_value;
      memcpy(row_buffer + buffer_offset, &int_value, 4);
      buffer_offset += 4;
    } else {
      // CHAR/VARCHAR(n) stored as fixed n with length tag
      if (current_token->tok_value != STRING_LITERAL) {
        rc = TYPE_MISMATCH;
        current_token->tok_value = INVALID;
        break;
      }
      int string_length = (int)strlen(current_token->tok_string);
      if (string_length <= 0 || string_length > current_column->col_len) {
        rc = INVALID_COLUMN_LENGTH;
        current_token->tok_value = INVALID;
        break;
      }
      row_buffer[buffer_offset++] = (unsigned char)string_length;
      memcpy(row_buffer + buffer_offset, current_token->tok_string,
             string_length);
      buffer_offset +=
          current_column->col_len; // advance fixed area (rest stays zero)
    }

    current_token = current_token->next;

    // comma or right paren
    if (column_index < table_descriptor->num_columns - 1) {
      if (current_token->tok_value != S_COMMA) {
        rc = INVALID_INSERT_DEFINITION;
        current_token->tok_value = INVALID;
        break;
      }
      current_token = current_token->next;
    } else {
      if (current_token->tok_value != S_RIGHT_PAREN) {
        rc = INVALID_INSERT_DEFINITION;
        current_token->tok_value = INVALID;
        break;
      }
      current_token = current_token->next;
    }
  }

  if (!rc) {
    if (current_token->tok_value != EOC) {
      rc = INVALID_STATEMENT;
      current_token->tok_value = INVALID;
    }
  }

  if (!rc) {
    long write_position = row_pos(&file_header, file_header.num_records);
    fseek(table_file, write_position, SEEK_SET);
    if (fwrite(row_buffer, record_size, 1, table_file) != 1)
      rc = FILE_WRITE_ERROR;
    else {
      file_header.num_records += 1;
      rc = write_header(table_file, &file_header);
    }
  }

  free(row_buffer);
  fclose(table_file);
  return rc;
}

int sem_delete(token_list *t_list) {
  int rc = 0;
  token_list *cur = t_list;

  // Validate table name
  if ((cur->tok_class != keyword) && (cur->tok_class != identifier) &&
      (cur->tok_class != type_name)) {
    rc = INVALID_TABLE_NAME;
    cur->tok_value = INVALID;
    return rc;
  }

  char table_name[MAX_IDENT_LEN + 1];
  strcpy(table_name, cur->tok_string);

  tpd_entry *tpd = get_tpd_from_list(table_name);
  if (!tpd) {
    rc = TABLE_NOT_EXIST;
    cur->tok_value = INVALID;
    return rc;
  }

  cur = cur->next;

  // Check for WHERE clause
  bool has_where = false;
  char where_column[MAX_IDENT_LEN + 1] = {0};
  int where_operator = 0;
  char where_value_str[256] = {0};
  int where_value_int = 0;
  int where_value_type = 0;

  if (cur->tok_value == K_WHERE) {
    has_where = true;
    cur = cur->next;

    if ((cur->tok_class != keyword) && (cur->tok_class != identifier) &&
        (cur->tok_class != type_name)) {
      rc = COLUMN_NOT_EXIST;
      cur->tok_value = INVALID;
      return rc;
    }
    strcpy(where_column, cur->tok_string);
    cur = cur->next;

    if (cur->tok_value != S_EQUAL && cur->tok_value != S_LESS &&
        cur->tok_value != S_GREATER) {
      rc = INVALID_STATEMENT;
      cur->tok_value = INVALID;
      return rc;
    }
    where_operator = cur->tok_value;
    cur = cur->next;

    if (cur->tok_value == STRING_LITERAL) {
      where_value_type = STRING_LITERAL;
      strcpy(where_value_str, cur->tok_string);
    } else if (cur->tok_value == INT_LITERAL) {
      where_value_type = INT_LITERAL;
      where_value_int = atoi(cur->tok_string);
    } else {
      rc = INVALID_STATEMENT;
      cur->tok_value = INVALID;
      return rc;
    }
    cur = cur->next;
  }

  if (cur->tok_value != EOC) {
    rc = INVALID_STATEMENT;
    cur->tok_value = INVALID;
    return rc;
  }

  // Find WHERE column index
  int where_col_index = -1;
  cd_entry *columns = (cd_entry *)((char *)tpd + tpd->cd_offset);
  if (has_where) {
    for (int i = 0; i < tpd->num_columns; i++) {
      if (strcasecmp(columns[i].col_name, where_column) == 0) {
        where_col_index = i;
        break;
      }
    }
    if (where_col_index == -1) {
      rc = COLUMN_NOT_EXIST;
      return rc;
    }
  }

  // Open table file
  FILE *fptr = NULL;
  table_file_header hdr;
  if ((rc = open_tab_rw(table_name, &fptr, &hdr)))
    return rc;

  int record_size = hdr.record_size;
  unsigned char *row_buffer = (unsigned char *)malloc(record_size);
  if (!row_buffer) {
    fclose(fptr);
    return MEMORY_ERROR;
  }

  bool *keep_row = (bool *)calloc(hdr.num_records, sizeof(bool));
  if (!keep_row) {
    free(row_buffer);
    fclose(fptr);
    return MEMORY_ERROR;
  }

  int deleted_count = 0;

  for (int row_idx = 0; row_idx < hdr.num_records; row_idx++) {
    fseek(fptr, row_pos(&hdr, row_idx), SEEK_SET);
    if (fread(row_buffer, record_size, 1, fptr) != 1) {
      rc = FILE_OPEN_ERROR;
      break;
    }

    bool delete_this_row = !has_where;

    if (has_where) {
      int offset = 0;
      for (int col = 0; col < tpd->num_columns; col++) {
        unsigned char len = row_buffer[offset++];
        if (col == where_col_index) {
          bool match = false;
          if (columns[col].col_type == T_INT) {
            if (len != 0 && where_value_type == INT_LITERAL) {
              int32_t row_int;
              memcpy(&row_int, row_buffer + offset, 4);
              if (where_operator == S_EQUAL)
                match = (row_int == where_value_int);
              else if (where_operator == S_LESS)
                match = (row_int < where_value_int);
              else if (where_operator == S_GREATER)
                match = (row_int > where_value_int);
            }
            offset += 4;
          } else {
            if (len != 0 && where_value_type == STRING_LITERAL) {
              int cmp =
                  strncmp((char *)(row_buffer + offset), where_value_str, len);
              if (where_operator == S_EQUAL)
                match = (cmp == 0 && len == strlen(where_value_str));
              else if (where_operator == S_LESS)
                match = (cmp < 0);
              else if (where_operator == S_GREATER)
                match = (cmp > 0);
            }
            offset += columns[col].col_len;
          }
          delete_this_row = match;
          break;
        } else {
          if (columns[col].col_type == T_INT)
            offset += 4;
          else
            offset += columns[col].col_len;
        }
      }
    }

    if (delete_this_row) {
      deleted_count++;
    } else {
      keep_row[row_idx] = true;
    }
  }

  if (!rc) {
    if (deleted_count == 0) {
      printf("Warning: No rows deleted.\n");
    } else {
      int write_idx = 0;
      for (int row_idx = 0; row_idx < hdr.num_records; row_idx++) {
        if (keep_row[row_idx]) {
          fseek(fptr, row_pos(&hdr, row_idx), SEEK_SET);
          if (fread(row_buffer, record_size, 1, fptr) != 1) {
            rc = FILE_OPEN_ERROR;
            break;
          }
          fseek(fptr, row_pos(&hdr, write_idx), SEEK_SET);
          if (fwrite(row_buffer, record_size, 1, fptr) != 1) {
            rc = FILE_WRITE_ERROR;
            break;
          }
          write_idx++;
        }
      }

      if (!rc) {
        hdr.num_records = hdr.num_records - deleted_count;
        rc = write_header(fptr, &hdr);
        printf("%d row(s) deleted.\n", deleted_count);
      }
    }
  }

  free(keep_row);
  free(row_buffer);
  fclose(fptr);
  return rc;
}

int sem_update(token_list *t_list) {
  int rc = 0;
  token_list *cur = t_list;

  if ((cur->tok_class != keyword) && (cur->tok_class != identifier) &&
      (cur->tok_class != type_name)) {
    rc = INVALID_TABLE_NAME;
    cur->tok_value = INVALID;
    return rc;
  }

  char table_name[MAX_IDENT_LEN + 1];
  strcpy(table_name, cur->tok_string);

  tpd_entry *tpd = get_tpd_from_list(table_name);
  if (!tpd) {
    rc = TABLE_NOT_EXIST;
    cur->tok_value = INVALID;
    return rc;
  }

  cur = cur->next;

  if (cur->tok_value != K_SET) {
    rc = INVALID_STATEMENT;
    cur->tok_value = INVALID;
    return rc;
  }

  cur = cur->next;

  // Column to update
  if ((cur->tok_class != keyword) && (cur->tok_class != identifier) &&
      (cur->tok_class != type_name)) {
    rc = INVALID_COLUMN_NAME;
    cur->tok_value = INVALID;
    return rc;
  }

  char set_col_name[MAX_IDENT_LEN + 1];
  strcpy(set_col_name, cur->tok_string);

  // Verify column exists
  int set_col_idx = -1;
  cd_entry *columns = (cd_entry *)((char *)tpd + tpd->cd_offset);
  for (int i = 0; i < tpd->num_columns; i++) {
    if (strcasecmp(columns[i].col_name, set_col_name) == 0) {
      set_col_idx = i;
      break;
    }
  }
  if (set_col_idx == -1) {
    rc = COLUMN_NOT_EXIST;
    cur->tok_value = INVALID;
    return rc;
  }

  cur = cur->next;
  if (cur->tok_value != S_EQUAL) {
    rc = INVALID_STATEMENT;
    cur->tok_value = INVALID;
    return rc;
  }

  cur = cur->next;

  // Value to set
  int set_val_type = 0;
  int32_t set_val_int = 0;
  char set_val_str[256] = {0};

  if (cur->tok_value == INT_LITERAL) {
    set_val_type = INT_LITERAL;
    set_val_int = atoi(cur->tok_string);
    if (columns[set_col_idx].col_type != T_INT) {
      rc = TYPE_MISMATCH;
      cur->tok_value = INVALID;
      return rc;
    }
  } else if (cur->tok_value == STRING_LITERAL) {
    set_val_type = STRING_LITERAL;
    strcpy(set_val_str, cur->tok_string);
    if (columns[set_col_idx].col_type == T_INT) {
      rc = TYPE_MISMATCH;
      cur->tok_value = INVALID;
      return rc;
    }
    if (strlen(set_val_str) > columns[set_col_idx].col_len) {
      rc = INVALID_COLUMN_LENGTH;
      cur->tok_value = INVALID;
      return rc;
    }
  } else if (cur->tok_value == K_NULL) {
    // Handle NULL if allowed
    if (columns[set_col_idx].not_null) {
      rc = NOT_NULL_CONSTRAINT_VIOLATION;
      cur->tok_value = INVALID;
      return rc;
    }
    set_val_type = K_NULL;
  } else {
    rc = INVALID_UPDATE_DEFINITION;
    cur->tok_value = INVALID;
    return rc;
  }

  cur = cur->next;

  // Check for WHERE
  bool has_where = false;
  char where_column[MAX_IDENT_LEN + 1] = {0};
  int where_operator = 0;
  char where_value_str[256] = {0};
  int where_value_int = 0;
  int where_value_type = 0;
  int where_col_index = -1;

  if (cur->tok_value == K_WHERE) {
    has_where = true;
    cur = cur->next;

    if ((cur->tok_class != keyword) && (cur->tok_class != identifier) &&
        (cur->tok_class != type_name)) {
      rc = COLUMN_NOT_EXIST;
      cur->tok_value = INVALID;
      return rc;
    }
    strcpy(where_column, cur->tok_string);

    // Find WHERE column index
    for (int i = 0; i < tpd->num_columns; i++) {
      if (strcasecmp(columns[i].col_name, where_column) == 0) {
        where_col_index = i;
        break;
      }
    }
    if (where_col_index == -1) {
      rc = COLUMN_NOT_EXIST;
      cur->tok_value = INVALID;
      return rc;
    }

    cur = cur->next;
    if (cur->tok_value != S_EQUAL && cur->tok_value != S_LESS &&
        cur->tok_value != S_GREATER) {
      rc = INVALID_STATEMENT;
      cur->tok_value = INVALID;
      return rc;
    }
    where_operator = cur->tok_value;
    cur = cur->next;

    if (cur->tok_value == STRING_LITERAL) {
      where_value_type = STRING_LITERAL;
      strcpy(where_value_str, cur->tok_string);
    } else if (cur->tok_value == INT_LITERAL) {
      where_value_type = INT_LITERAL;
      where_value_int = atoi(cur->tok_string);
    } else {
      rc = INVALID_STATEMENT;
      cur->tok_value = INVALID;
      return rc;
    }
    cur = cur->next;
  }

  if (cur->tok_value != EOC) {
    rc = INVALID_STATEMENT;
    cur->tok_value = INVALID;
    return rc;
  }

  // Open file and update
  FILE *fptr = NULL;
  table_file_header hdr;
  if ((rc = open_tab_rw(table_name, &fptr, &hdr)))
    return rc;

  int record_size = hdr.record_size;
  unsigned char *row_buffer = (unsigned char *)malloc(record_size);
  if (!row_buffer) {
    fclose(fptr);
    return MEMORY_ERROR;
  }

  int updated_count = 0;

  for (int row_idx = 0; row_idx < hdr.num_records; row_idx++) {
    long pos = row_pos(&hdr, row_idx);
    fseek(fptr, pos, SEEK_SET);
    if (fread(row_buffer, record_size, 1, fptr) != 1) {
      rc = FILE_OPEN_ERROR;
      break;
    }

    bool update_row = !has_where;
    if (has_where) {
      int offset = 0;
      for (int col = 0; col < tpd->num_columns; col++) {
        unsigned char len = row_buffer[offset++];
        if (col == where_col_index) {
          bool match = false;
          if (columns[col].col_type == T_INT) {
            if (len != 0 && where_value_type == INT_LITERAL) {
              int32_t row_int;
              memcpy(&row_int, row_buffer + offset, 4);
              if (where_operator == S_EQUAL)
                match = (row_int == where_value_int);
              else if (where_operator == S_LESS)
                match = (row_int < where_value_int);
              else if (where_operator == S_GREATER)
                match = (row_int > where_value_int);
            }
            offset += 4;
          } else {
            // String comparison
            if (len > 0 && where_value_type == STRING_LITERAL) {
              char row_str[256] = {0};
              memcpy(row_str, row_buffer + offset, len);
              row_str[len] = '\0';
              int cmp = strcmp(row_str, where_value_str);
              if (where_operator == S_EQUAL)
                match = (cmp == 0);
              else if (where_operator == S_LESS)
                match = (cmp < 0);
              else if (where_operator == S_GREATER)
                match = (cmp > 0);
            }
            offset += columns[col].col_len;
          }
          update_row = match;
          break;
        } else {
          if (columns[col].col_type == T_INT)
            offset += 4;
          else
            offset += columns[col].col_len;
        }
      }
    }

    if (update_row) {
      // Update the column in row_buffer
      int offset = 0;
      for (int col = 0; col < tpd->num_columns; col++) {
        if (col == set_col_idx) {
          // Update this column
          if (set_val_type == K_NULL) {
            row_buffer[offset++] = 0; // length 0
            // Zero out payload
            if (columns[col].col_type == T_INT) {
              memset(row_buffer + offset, 0, 4);
              offset += 4;
            } else {
              memset(row_buffer + offset, 0, columns[col].col_len);
              offset += columns[col].col_len;
            }
          } else if (columns[col].col_type == T_INT) {
            row_buffer[offset++] = 4;
            memcpy(row_buffer + offset, &set_val_int, 4);
            offset += 4;
          } else {
            int len = strlen(set_val_str);
            row_buffer[offset++] = len;
            memcpy(row_buffer + offset, set_val_str, len);
            // Zero out remaining bytes if any
            if (len < columns[col].col_len) {
              memset(row_buffer + offset + len, 0, columns[col].col_len - len);
            }
            offset += columns[col].col_len;
          }
        } else {
          // Skip
          unsigned char len = row_buffer[offset++];
          if (columns[col].col_type == T_INT)
            offset += 4;
          else
            offset += columns[col].col_len;
        }
      }

      // Write back
      fseek(fptr, pos, SEEK_SET);
      if (fwrite(row_buffer, record_size, 1, fptr) != 1) {
        rc = FILE_WRITE_ERROR;
        break;
      }
      updated_count++;
    }
  }

  free(row_buffer);
  fclose(fptr);

  if (!rc) {
    if (updated_count == 0) {
      printf("Warning: No rows updated.\n");
    } else {
      printf("%d row(s) updated.\n", updated_count);
    }
  }

  return rc;
}

int sem_select(token_list *t_list) {
  int rc = 0;
  token_list *cur = t_list;
  
  // Query state variables
  bool is_star = false;
  bool is_aggregate = false;
  aggregate_func agg_funcs[MAX_NUM_COL];
  int num_agg_funcs = 0;
  select_column sel_cols[MAX_NUM_COL];
  int num_sel_cols = 0;

  // 1. Parse SELECT list (star, aggregates, or column list)
  if (cur->tok_value == S_STAR) {
    is_star = true;
    cur = cur->next;
  } else if (cur->tok_class == function_name) {
    // Parse one or more aggregate functions (e.g., SUM(x), AVG(y))
    is_aggregate = true;
    
    do {
      if (cur->tok_class != function_name) {
        return INVALID_SELECT_DEFINITION;
      }
      
      int func_type = cur->tok_value;
      cur = cur->next;
      
      // Expect opening parenthesis
      if (cur->tok_value != S_LEFT_PAREN) {
        return INVALID_SELECT_DEFINITION;
      }
      cur = cur->next;
      
      // Parse aggregate parameter (column name or *)
      char param_name[MAX_IDENT_LEN + 1] = {0};
      if (cur->tok_value == S_STAR) {
        if (func_type != F_COUNT) {
          return INVALID_SELECT_DEFINITION; // Only COUNT(*) is valid
        }
        strcpy(param_name, "*");
        cur = cur->next;
      } else if (cur->tok_class == keyword || cur->tok_class == identifier ||
                 cur->tok_class == type_name) {
        strcpy(param_name, cur->tok_string);
        cur = cur->next;
      } else {
        return INVALID_SELECT_DEFINITION;
      }
      
      // Expect closing parenthesis
      if (cur->tok_value != S_RIGHT_PAREN) {
        return INVALID_SELECT_DEFINITION;
      }
      cur = cur->next;
      
      // Store aggregate function
      agg_funcs[num_agg_funcs].type = func_type;
      strcpy(agg_funcs[num_agg_funcs].col_name, param_name);
      num_agg_funcs++;
      
      // Check for comma indicating more aggregates
      if (cur->tok_value == S_COMMA) {
        cur = cur->next;
      } else {
        break;
      }
    } while (cur->tok_class == function_name);
    
    // If we saw a comma but no function follows, it's an error
    if (cur->tok_value == S_COMMA || 
        (num_agg_funcs > 0 && cur[-1].tok_value == S_COMMA)) {
      return INVALID_SELECT_DEFINITION;
    }
  } else {
    // Parse list of column names
    do {
      if (cur->tok_class != keyword && cur->tok_class != identifier &&
          cur->tok_class != type_name) {
        return INVALID_SELECT_DEFINITION;
      }
      strcpy(sel_cols[num_sel_cols].name, cur->tok_string);
      num_sel_cols++;
      cur = cur->next;
      
      if (cur->tok_value == S_COMMA) {
        cur = cur->next;
      } else {
        break;
      }
    } while (true);
  }

  // 2. Parse FROM
  if (cur->tok_value != K_FROM) {
    return INVALID_STATEMENT;
  }
  cur = cur->next;

  char table1[MAX_IDENT_LEN + 1];
  if ((cur->tok_class != keyword) && (cur->tok_class != identifier) &&
      (cur->tok_class != type_name)) {
    return INVALID_TABLE_NAME;
  }
  strcpy(table1, cur->tok_string);
  cur = cur->next;

  tpd_entry *tpd1 = get_tpd_from_list(table1);
  if (!tpd1) {
    return TABLE_NOT_EXIST;
  }

  // Validate aggregate functions on appropriate column types
  if (is_aggregate) {
    cd_entry *cols = (cd_entry *)((char *)tpd1 + tpd1->cd_offset);
    for (int i = 0; i < num_agg_funcs; i++) {
      if (agg_funcs[i].type == F_SUM || agg_funcs[i].type == F_AVG) {
        if (strcmp(agg_funcs[i].col_name, "*") != 0) {
          // Find the column and check if it's INT type
          bool found = false;
          for (int k = 0; k < tpd1->num_columns; k++) {
            if (strcasecmp(cols[k].col_name, agg_funcs[i].col_name) == 0) {
              found = true;
              if (cols[k].col_type != T_INT) {
                printf("Error: SUM and AVG can only be used on integer columns\n");
                return INVALID_SELECT_DEFINITION;
              }
              break;
            }
          }
          if (!found) {
            return COLUMN_NOT_EXIST;
          }
        }
      }
    }
  }

  // 3. Parse NATURAL JOIN (Optional)
  bool has_join = false;
  char table2[MAX_IDENT_LEN + 1] = {0};
  tpd_entry *tpd2 = NULL;

  if (cur->tok_value == K_NATURAL) {
    cur = cur->next;
    if (cur->tok_value != K_JOIN) {
      return INVALID_STATEMENT;
    }
    cur = cur->next;
    if ((cur->tok_class != keyword) && (cur->tok_class != identifier) &&
        (cur->tok_class != type_name)) {
      return INVALID_TABLE_NAME;
    }
    strcpy(table2, cur->tok_string);
    tpd2 = get_tpd_from_list(table2);
    if (!tpd2) {
      return TABLE_NOT_EXIST;
    }
    has_join = true;
    cur = cur->next;
  }

  // 4. Parse WHERE clause (optional)
  query_condition conditions[10];  // Support up to 10 conditions
  int num_conditions = 0;

  if (cur->tok_value == K_WHERE) {
    cur = cur->next;
    do {
      // Parse column name
      if (cur->tok_class != keyword && cur->tok_class != identifier &&
          cur->tok_class != type_name) {
        return COLUMN_NOT_EXIST;
      }
      strcpy(conditions[num_conditions].col_name, cur->tok_string);
      cur = cur->next;

      // Parse operator and value
      if (cur->tok_value == K_IS) {
        conditions[num_conditions].operator_type = K_IS;
        cur = cur->next;
        if (cur->tok_value == K_NULL) {
          conditions[num_conditions].value_type = K_NULL;
          cur = cur->next;
        } else if (cur->tok_value == K_NOT) {
          cur = cur->next;
          if (cur->tok_value == K_NULL) {
            conditions[num_conditions].value_type = K_NOT;  // IS NOT NULL
            cur = cur->next;
          } else {
            return INVALID_STATEMENT;
          }
        } else {
          return INVALID_STATEMENT;
        }
      } else if (cur->tok_value == S_EQUAL || cur->tok_value == S_LESS ||
                 cur->tok_value == S_GREATER) {
        conditions[num_conditions].operator_type = cur->tok_value;
        cur = cur->next;
        
        // Parse value
        if (cur->tok_value == INT_LITERAL) {
          conditions[num_conditions].value_type = INT_LITERAL;
          conditions[num_conditions].int_value = atoi(cur->tok_string);
          cur = cur->next;
        } else if (cur->tok_value == STRING_LITERAL) {
          conditions[num_conditions].value_type = STRING_LITERAL;
          strcpy(conditions[num_conditions].str_value, cur->tok_string);
          cur = cur->next;
        } else {
          return INVALID_STATEMENT;
        }
        
        // Validate type compatibility between column and value
        // For JOIN scenarios, we'll defer validation since column might be in table2
        // For now, check if column exists in table1
        cd_entry *cols_check = (cd_entry *)((char *)tpd1 + tpd1->cd_offset);
        int col_idx = -1;
        for (int k = 0; k < tpd1->num_columns; k++) {
          if (strcasecmp(cols_check[k].col_name, conditions[num_conditions].col_name) == 0) {
            col_idx = k;
            break;
          }
        }
        
        // If column found in table1, validate type compatibility
        if (col_idx != -1) {
          // Type mismatch validation
          if (cols_check[col_idx].col_type == T_INT && 
              conditions[num_conditions].value_type == STRING_LITERAL) {
            printf("Error: Type mismatch - cannot compare integer column with string value\n");
            return TYPE_MISMATCH;
          }
          if (cols_check[col_idx].col_type != T_INT && 
              conditions[num_conditions].value_type == INT_LITERAL) {
            printf("Error: Type mismatch - cannot compare string column with integer value\n");
            return TYPE_MISMATCH;
          }
        }
        // If column not in table1 and there's no JOIN yet parsed, it's an error
        // But since we haven't parsed JOIN yet at this point, we'll defer this check
        // The column existence will be validated during execution
      } else {
        return INVALID_STATEMENT;
      }

      // Check for logical operators (AND/OR)
      if (cur->tok_value == K_AND || cur->tok_value == K_OR) {
        conditions[num_conditions].logical_operator = cur->tok_value;
        num_conditions++;
        cur = cur->next;
      } else {
        conditions[num_conditions].logical_operator = 0;  // Last condition
        num_conditions++;
        break;
      }
    } while (true);
  }

  // 5. Parse ORDER BY (Optional)
  bool has_order = false;
  char order_col[MAX_IDENT_LEN + 1] = {0};
  bool order_desc = false;

  if (cur->tok_value == K_ORDER) {
    cur = cur->next;
    if (cur->tok_value != K_BY) {
      return INVALID_STATEMENT;
    }
    cur = cur->next;
    if ((cur->tok_class != keyword) && (cur->tok_class != identifier) &&
        (cur->tok_class != type_name)) {
      return INVALID_COLUMN_NAME;
    }
    strcpy(order_col, cur->tok_string);
    cur = cur->next;
    if (cur->tok_value == K_DESC) {
      order_desc = true;
      cur = cur->next;
    }
    has_order = true;
  }

  if (cur->tok_value != EOC) {
    return INVALID_STATEMENT;
  }

  // Execution
  // We need to load data, join if needed, filter, store, sort, aggregate/print.

  // Define a generic row structure for memory storage
  struct ResultRow {
    unsigned char *data; // Contains all columns from t1 (and t2 if join)
    int size;
  };

  // We will store pointers to dynamically allocated row data
  // Since MAX_ROWS is small (100), we can just use a fixed array or simple
  // dynamic array. But wait, join can produce more. Let's assume a reasonable
  // limit or use malloc. For this project, let's use a dynamic list.

  struct ResultRow *results = NULL;
  int result_count = 0;
  int result_capacity = 0;

  auto add_result = [&](unsigned char *row_data, int size) {
    if (result_count >= result_capacity) {
      result_capacity = (result_capacity == 0) ? 128 : result_capacity * 2;
      results = (struct ResultRow *)realloc(
          results, result_capacity * sizeof(struct ResultRow));
    }
    results[result_count].data = (unsigned char *)malloc(size);
    memcpy(results[result_count].data, row_data, size);
    results[result_count].size = size;
    result_count++;
  };

  // Helper to check conditions
  auto check_condition = [&](unsigned char *row, tpd_entry *tpd,
                             int offset_base) -> bool {
    // For join, we might need to look up columns in tpd1 or tpd2.
    // But to simplify, we can construct a "virtual" tpd or just look up in
    // both. Actually, for join, the row data is concatenated? Or we pass two
    // rows? Let's assume we construct a single combined row for join.

    // Wait, constructing combined row is expensive if we discard it.
    // Better to pass tpd and row.
    // If join, we have tpd1, tpd2 and row part 1, row part 2.
    // Let's simplify: always construct a combined row for processing if join.
    return true;
  };

  // We need column info for processing
  cd_entry *cols1 = (cd_entry *)((char *)tpd1 + tpd1->cd_offset);
  cd_entry *cols2 =
      has_join ? (cd_entry *)((char *)tpd2 + tpd2->cd_offset) : NULL;

  // Open files
  FILE *f1 = NULL, *f2 = NULL;
  table_file_header h1, h2;
  if ((rc = open_tab_rw(table1, &f1, &h1)))
    return rc;
  if (has_join) {
    if ((rc = open_tab_rw(table2, &f2, &h2))) {
      fclose(f1);
      return rc;
    }
  }

  unsigned char *buf1 = (unsigned char *)malloc(h1.record_size);
  unsigned char *buf2 =
      has_join ? (unsigned char *)malloc(h2.record_size) : NULL;

  // Identify common columns for join
  int common1[MAX_NUM_COL], common2[MAX_NUM_COL];
  int num_common = 0;
  if (has_join) {
    num_common = find_common_columns(tpd1, tpd2, common1, common2);
    if (num_common == 0) {
      printf("Warning: No common columns found for NATURAL JOIN\n");
      // Proceed as cross product? Or empty? Standard natural join is empty if
      // no common cols? Actually usually it's cross product if no common cols,
      // but here let's assume empty or error? The prompt says "Natural Join",
      // usually implies equality on common columns. If no common columns, it's
      // a Cartesian product. Let's assume we proceed.
    }
  }

  // Loop and Filter
  for (int i = 0; i < h1.num_records; i++) {
    fseek(f1, row_pos(&h1, i), SEEK_SET);
    fread(buf1, h1.record_size, 1, f1);

    if (!has_join) {
      // Single table
      bool match = true;
      if (num_conditions > 0) {
        // Evaluate conditions
        // We support AND/OR. We need to handle precedence or just linear.
        // Let's assume linear left-to-right for simplicity or strict precedence
        // if needed. Given the structure: cond1 [OP cond2], let's evaluate
        // sequentially. Actually, we need to handle the logical operators.
        // result = cond1; if (op1 == AND) result = result && cond2; else result
        // = result || cond2;

        bool current_res = false;
        // Evaluate first condition
        // Helper to eval one condition
        auto eval = [&](int cond_idx, unsigned char *row,
                        tpd_entry *tpd) -> bool {
          query_condition *c = &conditions[cond_idx];
          // Find column
          int col_idx = -1;
          cd_entry *cols = (cd_entry *)((char *)tpd + tpd->cd_offset);
          for (int k = 0; k < tpd->num_columns; k++) {
            if (strcasecmp(cols[k].col_name, c->col_name) == 0) {
              col_idx = k;
              break;
            }
          }
          if (col_idx == -1)
            return false; // Should be caught earlier but safety

          int offset = 0;
          for (int k = 0; k < col_idx; k++) {
            if (cols[k].col_type == T_INT)
              offset += 1 + 4;
            else
              offset += 1 + cols[k].col_len;
          }

          unsigned char len = row[offset++];

          if (c->operator_type == K_IS) {
            if (c->value_type == K_NULL)
              return (len == 0);
            if (c->value_type == K_NOT)
              return (len != 0);
          }

          if (len == 0)
            return false; // NULL fails other comparisons

          if (cols[col_idx].col_type == T_INT) {
            int val;
            memcpy(&val, row + offset, 4);
            if (c->operator_type == S_EQUAL)
              return val == c->int_value;
            if (c->operator_type == S_LESS)
              return val < c->int_value;
            if (c->operator_type == S_GREATER)
              return val > c->int_value;
          } else {
            char val_str[256] = {0};
            memcpy(val_str, row + offset, len);
            int cmp = strcmp(val_str, c->str_value);
            if (c->operator_type == S_EQUAL)
              return cmp == 0;
            if (c->operator_type == S_LESS)
              return cmp < 0;
            if (c->operator_type == S_GREATER)
              return cmp > 0;
          }
          return false;
        };

        current_res = eval(0, buf1, tpd1);
        for (int k = 0; k < num_conditions - 1; k++) {
          bool next_res = eval(k + 1, buf1, tpd1);
          if (conditions[k].logical_operator == K_AND)
            current_res = current_res && next_res;
          else if (conditions[k].logical_operator == K_OR)
            current_res = current_res || next_res;
        }
        match = current_res;
      }

      if (match) {
        add_result(buf1, h1.record_size);
      }
    } else {
      // Join
      for (int j = 0; j < h2.num_records; j++) {
        fseek(f2, row_pos(&h2, j), SEEK_SET);
        fread(buf2, h2.record_size, 1, f2);

        // Check join condition
        if (rows_match_on_common_columns(buf1, buf2, cols1, cols2, common1,
                                         common2, num_common)) {
          // Construct combined row for condition checking and storage
          // Combined row format: [Row1 Data] [Row2 Data]
          // This is a bit hacky but works for storage.
          // For condition checking, we need to know which table the column
          // belongs to. The prompt says "column_name" in WHERE. If ambiguous,
          // what happens? We'll search tpd1 first, then tpd2.

          bool match = true;
          if (num_conditions > 0) {
            bool current_res = false;
            auto eval_join = [&](int cond_idx) -> bool {
              query_condition *c = &conditions[cond_idx];
              // Try tpd1
              int col_idx = -1;
              bool in_t1 = true;
              for (int k = 0; k < tpd1->num_columns; k++) {
                if (strcasecmp(cols1[k].col_name, c->col_name) == 0) {
                  col_idx = k;
                  break;
                }
              }
              if (col_idx == -1) {
                in_t1 = false;
                for (int k = 0; k < tpd2->num_columns; k++) {
                  if (strcasecmp(cols2[k].col_name, c->col_name) == 0) {
                    col_idx = k;
                    break;
                  }
                }
              }
              if (col_idx == -1)
                return false;

              unsigned char *row = in_t1 ? buf1 : buf2;
              cd_entry *cols = in_t1 ? cols1 : cols2;

              int offset = 0;
              for (int k = 0; k < col_idx; k++) {
                if (cols[k].col_type == T_INT)
                  offset += 1 + 4;
                else
                  offset += 1 + cols[k].col_len;
              }
              unsigned char len = row[offset++];

              if (c->operator_type == K_IS) {
                if (c->value_type == K_NULL)
                  return (len == 0);
                if (c->value_type == K_NOT)
                  return (len != 0);
              }
              if (len == 0)
                return false;

              if (cols[col_idx].col_type == T_INT) {
                int val;
                memcpy(&val, row + offset, 4);
                if (c->operator_type == S_EQUAL)
                  return val == c->int_value;
                if (c->operator_type == S_LESS)
                  return val < c->int_value;
                if (c->operator_type == S_GREATER)
                  return val > c->int_value;
              } else {
                char val_str[256] = {0};
                memcpy(val_str, row + offset, len);
                int cmp = strcmp(val_str, c->str_value);
                if (c->operator_type == S_EQUAL)
                  return cmp == 0;
                if (c->operator_type == S_LESS)
                  return cmp < 0;
                if (c->operator_type == S_GREATER)
                  return cmp > 0;
              }
              return false;
            };

            current_res = eval_join(0);
            for (int k = 0; k < num_conditions - 1; k++) {
              bool next_res = eval_join(k + 1);
              if (conditions[k].logical_operator == K_AND)
                current_res = current_res && next_res;
              else if (conditions[k].logical_operator == K_OR)
                current_res = current_res || next_res;
            }
            match = current_res;
          }

          if (match) {
            // Store combined
            int size = h1.record_size + h2.record_size;
            unsigned char *combined = (unsigned char *)malloc(size);
            memcpy(combined, buf1, h1.record_size);
            memcpy(combined + h1.record_size, buf2, h2.record_size);
            add_result(combined, size);
            free(combined);
          }
        }
      }
    }
  }

  // Sort
  if (has_order && result_count > 1) {
    // We need to find the sort column type and offset
    // Similar lookup logic
    int sort_col_idx = -1;
    bool in_t1 = true;
    for (int k = 0; k < tpd1->num_columns; k++) {
      if (strcasecmp(cols1[k].col_name, order_col) == 0) {
        sort_col_idx = k;
        break;
      }
    }
    if (has_join && sort_col_idx == -1) {
      in_t1 = false;
      for (int k = 0; k < tpd2->num_columns; k++) {
        if (strcasecmp(cols2[k].col_name, order_col) == 0) {
          sort_col_idx = k;
          break;
        }
      }
    }

    if (sort_col_idx != -1) {
      // We need a custom comparator that knows how to extract the value
      // We can't pass captures to qsort, so we need a global or static, or use
      // a C++ sort with lambda? db.cpp is .cpp, so we can use std::sort! But we
      // need to include <algorithm> and <vector> maybe? The file uses
      // malloc/free, so let's stick to qsort if possible, but std::sort is
      // easier with lambdas. Let's assume we can use std::sort. But wait, I
      // can't easily add includes at the top without viewing the file again.
      // I'll use a simple bubble sort or selection sort since N is small (100).
      // Bubble sort is fine for 100 items.

      for (int i = 0; i < result_count - 1; i++) {
        for (int j = 0; j < result_count - i - 1; j++) {
          // Compare results[j] and results[j+1]
          unsigned char *r1 = results[j].data;
          unsigned char *r2 = results[j + 1].data;

          // Extract val1
          unsigned char *row1 = in_t1 ? r1 : (r1 + h1.record_size);
          cd_entry *cols = in_t1 ? cols1 : cols2;
          int off1 = 0;
          for (int k = 0; k < sort_col_idx; k++) {
            if (cols[k].col_type == T_INT)
              off1 += 5;
            else
              off1 += 1 + cols[k].col_len;
          }
          unsigned char len1 = row1[off1++];

          // Extract val2
          unsigned char *row2 = in_t1 ? r2 : (r2 + h1.record_size);
          int off2 = 0;
          for (int k = 0; k < sort_col_idx; k++) {
            if (cols[k].col_type == T_INT)
              off2 += 5;
            else
              off2 += 1 + cols[k].col_len;
          }
          unsigned char len2 = row2[off2++];

          int cmp = 0;
          if (cols[sort_col_idx].col_type == T_INT) {
            int v1 = 0, v2 = 0;
            if (len1 > 0)
              memcpy(&v1, row1 + off1, 4);
            if (len2 > 0)
              memcpy(&v2, row2 + off2, 4);
            if (v1 < v2)
              cmp = -1;
            else if (v1 > v2)
              cmp = 1;
          } else {
            char s1[256] = {0}, s2[256] = {0};
            if (len1 > 0)
              memcpy(s1, row1 + off1, len1);
            if (len2 > 0)
              memcpy(s2, row2 + off2, len2);
            cmp = strcmp(s1, s2);
          }

          if (order_desc)
            cmp = -cmp;

          if (cmp > 0) {
            // Swap
            struct ResultRow temp = results[j];
            results[j] = results[j + 1];
            results[j + 1] = temp;
          }
        }
      }
    }
  }

  // Output results: either aggregate or row-by-row
  if (is_aggregate) {
    // Structure to hold aggregate computation results
    struct AggregateResult {
      long long sum_value;
      int row_count;
      int column_index;
      bool is_in_table1;
    };
    AggregateResult agg_results[MAX_NUM_COL];
    
    // Initialize aggregate results and locate columns
    for (int a = 0; a < num_agg_funcs; a++) {
      agg_results[a].sum_value = 0;
      agg_results[a].row_count = 0;
      agg_results[a].column_index = -1;
      agg_results[a].is_in_table1 = true;
      
      // For COUNT(*), no column lookup needed
      if (agg_funcs[a].type == F_COUNT && strcmp(agg_funcs[a].col_name, "*") == 0) {
        continue;
      }
      
      // Locate column in table1
      for (int k = 0; k < tpd1->num_columns; k++) {
        if (strcasecmp(cols1[k].col_name, agg_funcs[a].col_name) == 0) {
          agg_results[a].column_index = k;
          break;
        }
      }
      
      // If not found and we have a join, look in table2
      if (has_join && agg_results[a].column_index == -1) {
        agg_results[a].is_in_table1 = false;
        for (int k = 0; k < tpd2->num_columns; k++) {
          if (strcasecmp(cols2[k].col_name, agg_funcs[a].col_name) == 0) {
            agg_results[a].column_index = k;
            break;
          }
        }
      }
      
      // Column not found (error unless it's COUNT which can be flexible)
      if (agg_results[a].column_index == -1 && agg_funcs[a].type != F_COUNT) {
        return INVALID_COLUMN_NAME;
      }
    }

    // Compute aggregates by processing each result row
    for (int i = 0; i < result_count; i++) {
      for (int a = 0; a < num_agg_funcs; a++) {
        // COUNT(*) just counts rows
        if (agg_funcs[a].type == F_COUNT && strcmp(agg_funcs[a].col_name, "*") == 0) {
          agg_results[a].row_count++;
          continue;
        }
        
        // Locate data in the appropriate table
        unsigned char *row_data = results[i].data;
        if (!agg_results[a].is_in_table1) {
          row_data += h1.record_size;  // Skip to table2 data
        }
        cd_entry *cols = agg_results[a].is_in_table1 ? cols1 : cols2;

        // Calculate offset to the target column
        int offset = 0;
        for (int k = 0; k < agg_results[a].column_index; k++) {
          if (cols[k].col_type == T_INT)
            offset += 5;
          else
            offset += 1 + cols[k].col_len;
        }
        unsigned char field_length = row_data[offset++];

        // Process non-NULL values
        if (field_length > 0) {
          if (agg_funcs[a].type == F_COUNT) {
            agg_results[a].row_count++;
          } else {  // SUM or AVG
            int value;
            memcpy(&value, row_data + offset, 4);
            agg_results[a].sum_value += value;
            agg_results[a].row_count++;
          }
        }
      }
    }

    // Display aggregate results with proper formatting
    // Header row (left-justified, 10 chars per column)
    for (int a = 0; a < num_agg_funcs; a++) {
      const char *header = (agg_funcs[a].type == F_SUM) ? "SUM" :
                          (agg_funcs[a].type == F_AVG) ? "AVG" : "COUNT";
      printf("%-10s", header);
      if (a < num_agg_funcs - 1) printf(" ");
    }
    printf("\n");
    
    // Separator row
    for (int a = 0; a < num_agg_funcs; a++) {
      printf("----------");
      if (a < num_agg_funcs - 1) printf(" ");
    }
    printf("\n");
    
    // Value row (right-justified, 10 chars per column)
    for (int a = 0; a < num_agg_funcs; a++) {
      if (agg_funcs[a].type == F_SUM) {
        printf("%10lld", agg_results[a].sum_value);
      } else if (agg_funcs[a].type == F_AVG) {
        long long avg = (agg_results[a].row_count > 0) ? 
                        (agg_results[a].sum_value / agg_results[a].row_count) : 0;
        printf("%10lld", avg);
      } else {  // F_COUNT
        int count = (strcmp(agg_funcs[a].col_name, "*") == 0) ? 
                    result_count : agg_results[a].row_count;
        printf("%10d", count);
      }
      if (a < num_agg_funcs - 1) printf(" ");
    }
    printf("\n");

  } else {
    // Print Rows
    // Reuse print_join_header logic or similar?
    // We need to print selected columns or *

    // Construct output columns list
    // If *, use all columns (from t1 and t2 if join)
    // If list, lookup columns.

    // ... (Printing logic similar to print_join_header but dynamic)

    // Let's simplify: reuse print_join_header if * and join?
    // But we have filtered results in memory.
    // We need a print function that takes memory rows.

    // Let's implement a quick printer here.

    // 1. Determine output columns
    struct OutCol {
      char name[MAX_IDENT_LEN + 1];
      int type;
      int len;
      int offset;
      bool in_t1;
    };
    OutCol out_cols[MAX_NUM_COL * 2];
    int num_out = 0;

    if (is_star) {
      for (int k = 0; k < tpd1->num_columns; k++) {
        strcpy(out_cols[num_out].name, cols1[k].col_name);
        out_cols[num_out].type = cols1[k].col_type;
        out_cols[num_out].len = cols1[k].col_len;
        out_cols[num_out].in_t1 = true;
        // Calculate byte offset
        int off = 0;
        for (int m = 0; m < k; m++)
          off += (cols1[m].col_type == T_INT ? 5 : 1 + cols1[m].col_len);
        out_cols[num_out].offset = off;
        num_out++;
      }
      if (has_join) {
        for (int k = 0; k < tpd2->num_columns; k++) {
          strcpy(out_cols[num_out].name, cols2[k].col_name);
          out_cols[num_out].type = cols2[k].col_type;
          out_cols[num_out].len = cols2[k].col_len;
          out_cols[num_out].in_t1 = false;
          int off = 0;
          for (int m = 0; m < k; m++)
            off += (cols2[m].col_type == T_INT ? 5 : 1 + cols2[m].col_len);
          out_cols[num_out].offset = off;
          num_out++;
        }
      }
    } else {
      for (int i = 0; i < num_sel_cols; i++) {
        int idx = -1;
        bool in_t1 = true;
        for (int k = 0; k < tpd1->num_columns; k++) {
          if (strcasecmp(cols1[k].col_name, sel_cols[i].name) == 0) {
            idx = k;
            break;
          }
        }
        if (has_join && idx == -1) {
          in_t1 = false;
          for (int k = 0; k < tpd2->num_columns; k++) {
            if (strcasecmp(cols2[k].col_name, sel_cols[i].name) == 0) {
              idx = k;
              break;
            }
          }
        }
        if (idx != -1) {
          cd_entry *cols = in_t1 ? cols1 : cols2;
          strcpy(out_cols[num_out].name, cols[idx].col_name);
          out_cols[num_out].type = cols[idx].col_type;
          out_cols[num_out].len = cols[idx].col_len;
          out_cols[num_out].in_t1 = in_t1;
          int off = 0;
          for (int m = 0; m < idx; m++)
            off += (cols[m].col_type == T_INT ? 5 : 1 + cols[m].col_len);
          out_cols[num_out].offset = off;
          num_out++;
        }
      }
    }

    // Print Header
    int widths[MAX_NUM_COL * 2];
    for (int i = 0; i < num_out; i++) {
      int w = strlen(out_cols[i].name);
      if (out_cols[i].type == T_INT) {
        if (w < 5)
          w = 5;
      } else {
        if (w < out_cols[i].len)
          w = out_cols[i].len;
      }
      widths[i] = w;
      printf("%-*s ", w, out_cols[i].name);
    }
    printf("\n");
    for (int i = 0; i < num_out; i++) {
      for (int k = 0; k < widths[i]; k++)
        putchar('-');
      putchar(' ');
    }
    printf("\n");

    // Print Data
    for (int i = 0; i < result_count; i++) {
      for (int j = 0; j < num_out; j++) {
        unsigned char *row = results[i].data;
        if (!out_cols[j].in_t1)
          row += h1.record_size;

        int off = out_cols[j].offset;
        unsigned char len = row[off++];

        if (out_cols[j].type == T_INT) {
          if (len == 0)
            printf("%-*s ", widths[j], "NULL");
          else {
            int val;
            memcpy(&val, row + off, 4);
            printf("%*d ", widths[j], val);
          }
        } else {
          if (len == 0)
            printf("%-*s ", widths[j], "NULL");
          else {
            printf("%-*.*s ", widths[j], (int)len, (char *)(row + off));
          }
        }
      }
      printf("\n");
    }
    printf("\n %d record(s) selected.\n\n", result_count);
  }

  // Cleanup
  for (int i = 0; i < result_count; i++)
    free(results[i].data);
  free(results);
  free(buf1);
  if (buf2)
    free(buf2);
  fclose(f1);
  if (f2)
    fclose(f2);

  return rc;
}

int initialize_tpd_list() {
  int rc = 0;
  FILE *fhandle = NULL;
  //	struct _stat file_stat;
  struct stat file_stat;

  /* Open for read */
  if ((fhandle = fopen("dbfile.bin", "rbc")) == NULL) {
    if ((fhandle = fopen("dbfile.bin", "wbc")) == NULL) {
      rc = FILE_OPEN_ERROR;
    } else {
      g_tpd_list = NULL;
      g_tpd_list = (tpd_list *)calloc(1, sizeof(tpd_list));

      if (!g_tpd_list) {
        rc = MEMORY_ERROR;
      } else {
        g_tpd_list->list_size = sizeof(tpd_list);
        fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
        fflush(fhandle);
        fclose(fhandle);
      }
    }
  } else {
    /* There is a valid dbfile.bin file - get file size */
    //		_fstat(_fileno(fhandle), &file_stat);
    fstat(fileno(fhandle), &file_stat);
    printf("dbfile.bin size = %d\n", file_stat.st_size);

    g_tpd_list = (tpd_list *)calloc(1, file_stat.st_size);

    if (!g_tpd_list) {
      rc = MEMORY_ERROR;
    } else {
      fread(g_tpd_list, file_stat.st_size, 1, fhandle);
      fflush(fhandle);
      fclose(fhandle);

      if (g_tpd_list->list_size != file_stat.st_size) {
        rc = DBFILE_CORRUPTION;
      }
    }
  }

  return rc;
}

int add_tpd_to_list(tpd_entry *tpd) {
  int rc = 0;
  int old_size = 0;
  FILE *fhandle = NULL;

  if ((fhandle = fopen("dbfile.bin", "wbc")) == NULL) {
    rc = FILE_OPEN_ERROR;
  } else {
    old_size = g_tpd_list->list_size;

    if (g_tpd_list->num_tables == 0) {
      /* If this is an empty list, overlap the dummy header */
      g_tpd_list->num_tables++;
      g_tpd_list->list_size += (tpd->tpd_size - sizeof(tpd_entry));
      fwrite(g_tpd_list, old_size - sizeof(tpd_entry), 1, fhandle);
    } else {
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

int drop_tpd_from_list(char *tabname) {
  int rc = 0;
  tpd_entry *cur = &(g_tpd_list->tpd_start);
  int num_tables = g_tpd_list->num_tables;
  bool found = false;
  int count = 0;

  if (num_tables > 0) {
    while ((!found) && (num_tables-- > 0)) {
      if (strcasecmp(cur->table_name, tabname) == 0) {
        /* found it */
        found = true;
        int old_size = 0;
        FILE *fhandle = NULL;

        if ((fhandle = fopen("dbfile.bin", "wbc")) == NULL) {
          rc = FILE_OPEN_ERROR;
        } else {
          old_size = g_tpd_list->list_size;

          if (count == 0) {
            /* If this is the first entry */
            g_tpd_list->num_tables--;

            if (g_tpd_list->num_tables == 0) {
              /* This is the last table, null out dummy header */
              memset((void *)g_tpd_list, '\0', sizeof(tpd_list));
              g_tpd_list->list_size = sizeof(tpd_list);
              fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
            } else {
              /* First in list, but not the last one */
              g_tpd_list->list_size -= cur->tpd_size;

              /* First, write the 8 byte header */
              fwrite(g_tpd_list, sizeof(tpd_list) - sizeof(tpd_entry), 1,
                     fhandle);

              /* Now write everything starting after the cur entry */
              fwrite((char *)cur + cur->tpd_size,
                     old_size - cur->tpd_size -
                         (sizeof(tpd_list) - sizeof(tpd_entry)),
                     1, fhandle);
            }
          } else {
            /* This is NOT the first entry - count > 0 */
            g_tpd_list->num_tables--;
            g_tpd_list->list_size -= cur->tpd_size;

            /* First, write everything from beginning to cur */
            fwrite(g_tpd_list, ((char *)cur - (char *)g_tpd_list), 1, fhandle);

            /* Check if cur is the last entry. Note that g_tdp_list->list_size
               has already subtracted the cur->tpd_size, therefore it will
               point to the start of cur if cur was the last entry */
            if ((char *)g_tpd_list + g_tpd_list->list_size == (char *)cur) {
              /* If true, nothing else to write */
            } else {
              /* NOT the last entry, copy everything from the beginning of the
                 next entry which is (cur + cur->tpd_size) and the remaining
                 size */
              fwrite((char *)cur + cur->tpd_size,
                     old_size - cur->tpd_size -
                         ((char *)cur - (char *)g_tpd_list),
                     1, fhandle);
            }
          }

          fflush(fhandle);
          fclose(fhandle);
        }

      } else {
        if (num_tables > 0) {
          cur = (tpd_entry *)((char *)cur + cur->tpd_size);
          count++;
        }
      }
    }
  }

  if (!found) {
    rc = INVALID_TABLE_NAME;
  }

  return rc;
}

tpd_entry *get_tpd_from_list(char *tabname) {
  tpd_entry *tpd = NULL;
  tpd_entry *cur = &(g_tpd_list->tpd_start);
  int num_tables = g_tpd_list->num_tables;
  bool found = false;

  if (num_tables > 0) {
    while ((!found) && (num_tables-- > 0)) {
      if (strcasecmp(cur->table_name, tabname) == 0) {
        /* found it */
        found = true;
        tpd = cur;
      } else {
        if (num_tables > 0) {
          cur = (tpd_entry *)((char *)cur + cur->tpd_size);
        }
      }
    }
  }

  return tpd;
}