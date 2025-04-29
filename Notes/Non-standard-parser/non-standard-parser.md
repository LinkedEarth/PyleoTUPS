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
        |                                                |
+-------v--------------------+                         +----------v-----------+
| Mostly strings             |                         | cv == 0 and mode > 1 |
| mean of Numeric tokens <0.2|                         +----------+-----------+
+-------+--------------------+                           |
        |                                                |
+-------v--------+                  +--------------------v------------+
| header-only    |                  | extract_headers()               |
| short block    |                  | header_extent > 0?              |
+----------------+                  | if yes:                         |
                                    |    generate_df()                |
                                    | if no:                          |
                                    |    find previous header block   |
                                    |    match width → assign/fallback|
                                    +---------------------------------+

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


## Rules for Parsing:

| Pattern Detected                                  | Block Type       | Description                                      | Functions Triggered                              |
|--------------------------------------------------|------------------|--------------------------------------------------|--------------------------------------------------|
| All strings, token count mode == 1              | "narrative"      | Likely a paragraph or description                | compute_statistics                               |
| All strings, mode > 1, block < 5 lines          | "header-only"    | Isolated multiline header block                  | extract_headers                                  |
| cv == 0 and mode > 1                            | "complete-tabular"| Clearly delimited table                         | extract_headers → generate_df                    |
| cv > 0 but header_extent > 0, subblock cv == 0  | "complete-tabular"| Hierarchical table with uniform rows            | extract_headers → generate_df                    |
| cv > 0 and header extent > 0, subblock cv > 0   | "complete-tabular"| Jagged data, fallback by overlap                | extract_headers → assign_tokens_by_overlap       |
| cv > 0 and header extent == 0                   | "data"           | Needs previous header block                     | find_header_block → assign_tokens_by_overlap     |


## Glossary:
| Term | Simplified Meaning |
|:-----|:-------------------|
| Block | A group of consecutive non-empty lines from the file, treated as one unit (e.g., a paragraph, header, or table). |
| Delimiter | A way to split a line into tokens: single-space (`\s+`), multi-space (`\s{2,}`), or tab (`\t`). |
| Token | A word or number extracted from a line after splitting. |
| Mean Numeric Ratio | Average of count of numeric tokens per lines: tells if the block is mostly numbers (data) or words (narrative/header). |
| Mode Token Count | Most common number of tokens per line in a block: tells how structured the block is. |
| Coefficient of Variation (CV) | Measures how much the number of tokens per line varies. Low CV → uniform structure (likely table). |
| Header Extent | Number of lines at the start of a block considered as the table’s column names. |
| Subblock CV | CV calculated only over data lines after header: used when deciding if fallback is needed. |
| Overlap Assignment | If simple splitting fails, assign tokens into columns by their visual position (spacing-based matching). |



# Function Descriptions for NonStandardParser

## parse(file_path)
**Input:**
- `file_path` (str): Path to the NOAA `.txt` file to be parsed.

**Output:**
- List of `pandas.DataFrame` objects: Each representing a parsed table detected from the file.

**Description:**
This is the main entry point. It reads the file, splits it into logical blocks, processes each block to classify its type, and attempts to build a DataFrame for usable tables.  
If parsing fails completely (no tables found), raises `ParsingError`.

---

## _read_file()
**Input:**
- None (uses `self.file_path` stored at initialization).

**Output:**
- Updates `self.lines` (list of strings): Lines from the file.

**Description:**
Reads the entire file content into memory line-by-line.  
Handles basic file I/O exceptions. File lines are stored without modification for further processing.

---

## _split_lines()
**Input:**
- None (uses `self.lines`).

**Output:**
- List of cleaned lines.

**Description:**
Processes the raw lines to normalize formatting (removing unnecessary trailing spaces, normalizing newline characters) and prepares the lines for block segmentation.

---

## _segregate_blocks()
**Input:**
- None (operates on `self.lines`).

**Output:**
- `self.blocks` (list of dicts): Each dict represents a block with its own metadata, lines, and statistical properties.

**Description:**
Divides the file into "blocks" — groups of consecutive non-empty lines.  
Each block is a candidate for header, table, or narrative section.  
Line-level stats like token counts and numeric ratios are also calculated here for later classification.

---

## _compute_statistics(block)
**Input:**
- `block` (dict): A block containing its lines and empty metadata fields.

**Output:**
- Updates the block in-place with statistics:
  - Mean numeric ratio
  - Token counts under different delimiters
  - Variability (coefficient of variation)

**Description:**
Analyzes token structure of the block to help classify its type later.  
This function does not make any block-type decisions itself, only prepares numeric measures.

---

## _process_block(block)
**Input:**
- `block` (dict): A single logical block (from `segregate_blocks`).

**Output:**
- Updates the block:
  - Sets `block_type`
  - Adds `df` (pandas DataFrame) if applicable

**Description:**
Based on the computed statistics and header detection, determines if the block is a complete table, a header-only block, or narrative.  
If the structure is good, tries to build a DataFrame directly; otherwise, falls back to overlap assignment or skips the block.

---

## _detect_delimiter(lines)
**Input:**
- `lines` (list of str): List of lines in the block.

**Output:**
- Detected delimiter (`\t`, `\s{2,}`, or fallback).

**Description:**
Analyzes lines to determine the best token-splitting strategy.  
Chooses between tabs, multi-spaces, or generic spaces based on the consistency of token counts.

---

## _extract_headers(block, delimiter)
**Input:**
- `block` (dict): Block to extract header lines from.
- `delimiter` (str): Delimiter used for splitting.

**Output:**
- List of tokenized headers: Each header with its text and visual intervals.

**Description:**
Tries to extract column names from the top lines of the block.  
If multiple header lines exist, merges them intelligently.  
Returns clean header metadata for constructing a DataFrame later.

---

## _generate_df(headers, data_lines, delimiter)
**Input:**
- `headers` (list of dicts): Header token objects.
- `data_lines` (list of dicts): Tokenized data lines.
- `delimiter` (str): Delimiter for tokenizing.

**Output:**
- `pandas.DataFrame`

**Description:**
Constructs a DataFrame by aligning data rows under extracted headers.  
Handles padding (adding empty columns) or trimming (dropping excess tokens) if the number of headers doesn't perfectly match the number of tokens per row.

---

## _assign_tokens_by_overlap(headers, lines, delimiter)
**Input:**
- `headers` (list of dicts): Tokenized header objects with interval positions.
- `lines` (list of dicts): Tokenized lines to assign.
- `delimiter` (str): Delimiter used.

**Output:**
- `pandas.DataFrame`

**Description:**
When splitting fails to cleanly align tokens into columns, this function assigns tokens based on their visual character positions and header overlaps.  
Fallback method for messy tables.

---

## _merge_headers_by_overlap(token_maps)
**Input:**
- `token_maps` (list of lists): Each inner list contains token info from a header line.

**Output:**
- Merged headers (list of dicts).

**Description:**
Merges multiple header lines into one unified set of headers by finding horizontally overlapping text ranges across lines.  
Used when headers span multiple rows visually.

---

# Special One: detect_header_extent(block, delimiter)
**Input:**
- `block` (dict): Block of lines.
- `delimiter` (str): Delimiter pattern used to split lines.

**Output:**
- Tuple:
  - Number of lines from the block start that qualify as headers
  - Index of a "title" line (if present, else `None`)

**Description:**
Checks how many starting lines in a block can be considered as column headers based on token analysis.  
Handles special cases where the very first line might be a title (like "Table S1: Uranium-Thorium Ages").  
Returns both the extent of header lines and the title line index if detected.
