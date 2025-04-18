```
                   +----------------------+
                   |   parse(file_path)   |
                   +----------+-----------+
                              |
                   +----------v-----------+
                   |   read_file()        |
                   +----------+-----------+
                              |
                   +----------v-----------+
                   |   split_lines()      |
                   +----------+-----------+
                              |
                   +----------v-----------+
                   | segregate_blocks()   |
                   | - create blocks      |
                   | - collect line stats |
                   +----------+-----------+
                              |
                   +----------v-----------+
                   | For block in blocks  |
                   |   process_block()    |
                   +----------+-----------+
                              |
        +---------------------+--------------------------+
        |                                            |
+-------v--------+                         +----------v-----------+
| Mostly strings |                         | cv == 0 and mode > 1 |
+-------+--------+                         +----------+-----------+
        |                                            |
+-------v--------+                  +-----------------v----------------+
| header-only    |                  | extract_headers()                |
| short block    |                  | header_extent > 0?              |
+----------------+                  | if yes:                         |
                                   |    generate_df()                |
                                   | if no:                          |
                                   |    find previous header block   |
                                   |    match width → assign/fallback|
                                   +----------------------------------+

        +--------------------+
        | cv > 0 but headers |
        +--------+-----------+
                 |
        +--------v-----------+
        | Sub-block cv == 0  |
        | → generate_df()    |
        +--------+-----------+
                 |
        +--------v-----------+
        | Sub-block cv != 0  |
        | → assign_by_overlap|
        +--------------------+
```


## Block Types - Verbal Case Table

| Pattern Detected                                  | Block Type       | Description                                      | Functions Triggered                              |
|--------------------------------------------------|------------------|--------------------------------------------------|--------------------------------------------------|
| All strings, token count mode == 1              | "narrative"      | Likely a paragraph or description                | compute_statistics                               |
| All strings, mode > 1, block < 5 lines          | "header-only"    | Isolated multiline header block                  | extract_headers                                  |
| cv == 0 and mode > 1                            | "complete-tabular"| Clearly delimited table                         | extract_headers → generate_df                    |
| cv > 0 but header_extent > 0, subblock cv == 0  | "complete-tabular"| Hierarchical table with uniform rows            | extract_headers → generate_df                    |
| cv > 0 and header extent > 0, subblock cv > 0   | "complete-tabular"| Jagged data, fallback by overlap                | extract_headers → assign_tokens_by_overlap       |
| cv > 0 and header extent == 0                   | "data"           | Needs previous header block                     | find_header_block → assign_tokens_by_overlap     |


## Logical Condition Triggers

| Condition                                                             | Action                                                     |
|----------------------------------------------------------------------|------------------------------------------------------------|
| mean_numeric_single < 0.3 and mode_multispace == 1              | mark as narrative                                           |
| mean_numeric_single < 0.3 and mode_multispace > 1 and block < 5 | extract headers, mark as header-only                       |
| cv_multispace == 0 and mode_multispace > 1                      | extract headers and build df                               |
| cv > 0 and header_extent > 0 and subblock_cv == 0               | build df using headers                                     |
| cv > 0 and header_extent > 0 and subblock_cv > 0                | fallback: assign by interval overlap                       |
| cv > 0 and header_extent == 0                                   | check previous blocks for headers → fallback assignment    |


## 🔹 Function Execution Paths

| Step                      | Functions                                                       |
|---------------------------|------------------------------------------------------------------|
| File Parsing              | read_file(), split_lines()                                       |
| Block Extraction          | segregate_blocks()                                               |
| Stat Computation          | compute_statistics()                                             |
| Header Detection          | detect_header_extent(), generate_row_pattern()                  |
| Header Parsing            | extract_headers(), get_token_intervals_multi(), merge_headers() |
| Data Construction         | generate_df(), assign_tokens_by_overlap()                       |
| Block Classification      | process_block(), most_common(), safe_cv()                        |
| Fallback Block Handling   | find previous block with "header-only"                           |
