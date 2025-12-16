## Merging Dataframe with same row extents

### Problem:
Tables are sometimes visually split into vertical strips by empty columns. The parser's segmentation logic correctly identified these as separate blocks, but this fragmented the single logical table into two disjoint DataFrames.

### Process:
We introduced a post-processing step _merge_compatible_blocks that runs after initial extraction:
- Group blocks that reside on the same Sheet and share the exact same Vertical Span (Top/Bottom rows).
- Verify they share the same Header Structure (header_extent).
- Concatenate the DataFrames horizontally (axis=1), merge their headers, and mark the right-hand block as MERGED (so it is skipped in the final output).

### Results: 
docs/rarotonga2006.log

### Discussion:
- Shall we add a header agreement score? 
i.e. if two blocks side by side have primary keys within themselves, do we really need to contact the dfs?

---------

## Extract metadata from Narrative blocks:

### Process:
- Master Metadata: Any narrative text found before the first table on a sheet is collected and assigned to df.attrs['master_metadata']. This applies to the whole sheet.

- Table-Level Metadata: Any narrative text found between two tables is collected and assigned to the next table as df.attrs['table_metadata'].

### Results:
docs/frank1999.log

### Discussion/ Next Step:
- Narrative blocks currently are blocks which are single column length, or do not have much numerical values.
- A Narrative block can be a Metadata as well as a Title. 
- Making use of Stop Words can help us determine if a block shall be treated as a title or metadata
- For accuracy, it would be good enough to have an Small LM which checks the content of the Narrative blocks.

---------

## Segregate block with column spans:

### **1. Problem Statement**
**The "Fused Block" Issue**
In certain files (e.g., `SST_Indian_Ocean_Bard1997.xls`), multiple distinct datasets are often placed side-by-side on a single sheet, separated only by empty columns.
* **Issue origin:** The parser’s initial segmentation logic (BFS) often groups these into a single "Super-Block" because they are visually connected by a shared title row or diagonal proximity.
* **Issue:** This results in a single, massive DataFrame where columns repeat (e.g., `Depth, Age, SST, <Gap>, Depth, Age, SST`).
* **Inaccurate Result:** Crucially, these side-by-side tables often have different row counts. The parser captures the bounding box of the *tallest* table, leaving the shorter tables filled with "ghost rows" (NaNs) at the bottom.

### **2. Solution**
New version includes a **"Profile, Group, and Split"** algorithm (`_decompose_fused_blocks`) that detects vertical islands of data within a block and surgically separates them.

* **I: Column Profiling (Height Map)**
    We iterate through every column in the fused block and calculate its **"Valid Height"** (the row index of the last non-empty value).
    * *Input:* A 10-column DataFrame.
    * *Result:* `[85, 85, 85, 0, 43, 43, 43, 0, 26, 26]` (Note the 0s representing gap columns).

* **II: Span Identification (Grouping)**
    We group contiguous columns that share similar valid heights into **Spans**.
    * *Span A:* Cols 0–2 (Height ~85)
    * *Span B:* Cols 4–6 (Height ~43)
    * *Span C:* Cols 8–9 (Height ~26)

* **III: Pattern Verification (Decision Layer)**
    To distinguish between a "Table with Notes"/"Single table with an intentional empty column" (keep fused) and "Repeated Tables" (split), we compare the **Header Sequences**.
    * **Logic:** If `Headers(Span A)` matches `Headers(Span B)` (e.g., both start with "Depth"), we confirm a **Split**.

* **IV: Vertical Slice & Horizontal Trim**
    For each confirmed Span, we perform a two-dimensional crop:
    1.  **Vertical:** Slice the columns defined by the Span (separating Table A from Table B).
    2.  **Horizontal:** Crop the rows to the Span’s specific median height.
        * *Result:* Table B is correctly cropped to 43 rows, removing the 42 rows of NaNs it inherited from Table A.

### **3. System Integration**
This component injects a new stage into the `ExcelParser` pipeline.

* **Pipeline Order:**
    1.  `_segregate_blocks` (Initial discovery)
    2.  `_process_block` (Raw DataFrame creation)
    3.  `_merge_compatible_blocks` (Stitching strips)
    4.  **`_decompose_fused_blocks` (Task 4: Splitting fused tables)** $\leftarrow$ *New Step*
    5.  `_attach_metadata` (Contextualization)

* **State Mutation:**
    * The **Original Block** object is mutated in-place to contain only the first dataset (Span A), shrinking its boundaries.
    * **New Block** objects are created for subsequent datasets (Span B, Span C) and **appended** to the end of the `self.blocks` list.

* **Robustness:**
    Because this runs *before* Metadata Attachment, the newly created blocks are fully available to receive their own specific narrative context in the final step.