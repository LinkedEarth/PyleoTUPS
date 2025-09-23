## GOAL:
Build a parser which works on the initially rejected corpus from NOAA. Majority of the files are excel sheets, with first sheet as README, and next sheets including the METADATA (Title + Abstract), and TABLE/s (Multi tables per sheet, Multi Line Header, etc)

## OBJECTIVES:
1. Try to analyse which methods from current parsers can be reused, and give a covergae percentage. 
2. If the current parsers can't be reused, find methods to trace back the ground truth NOAA files.

## CURRENT METHODS:

### 1. Standard Parser (NOAA Templated files)
1. Find Metadata 
2. Find Variables from: 
    a. Meta Data
    b. Data Section
    c. Create unnamed_0, unnamed_1, etc
3. Parse Data 
    a. Based on max_len of row, pad the data
4. Construct Data
    a. 
    b. if len(max_len_row) == # variables -> Safe construct df
    c. if len(max_len_row) < # variables -> Pad Data to construct df
    d. if len(max_len_row) > # variables -> Trim data to construct df

### 2. NonStandard Parser (Formatless Text File)
1. Segment text to block
2. Compute statistics based on various delimiter
3. For each block, Check if a block contains a table
    a. If yes, extract headers with respective intervals
    b. Parse data and assign it to headers based on interval overlap
    c. Construct df by matching data to respective headers

#### @TODO:
- Eyeball analysis of files
- Change block Segmentation method 
- Reuse logic as per NonStandard Parser > 3.

### Workflow:

## GOAL:
Build a parser which works on the initially rejected corpus from NOAA. Majority of the files are excel sheets, with first sheet as README, and next sheets including the METADATA (Title + Abstract), and TABLE/s (Multi tables per sheet, Multi Line Header, etc)

## OBJECTIVES:
1. Try to analyse which methods from current parsers can be reused, and give a covergae percentage. 
2. If the current parsers can't be reused, find methods to trace back the ground truth NOAA files.

## CURRENT METHODS:

### 1. Standard Parser (NOAA Templated files)
1. Find Metadata 
2. Find Variables from: 
    a. Meta Data
    b. Data Section
    c. Create unnamed_0, unnamed_1, etc
3. Parse Data 
    a. Based on max_len of row, pad the data
4. Construct Data
    a. 
    b. if len(max_len_row) == # variables -> Safe construct df
    c. if len(max_len_row) < # variables -> Pad Data to construct df
    d. if len(max_len_row) > # variables -> Trim data to construct df

### 2. NonStandard Parser (Formatless Text File)
1. Segment text to block
2. Compute statistics based on various delimiter
3. For each block, Check if a block contains a table
    a. If yes, extract headers with respective intervals
    b. Parse data and assign it to headers based on interval overlap
    c. Construct df by matching data to respective headers

#### @TODO:
- Eyeball analysis of files
- Change block Segmentation method 
- Reuse logic as per NonStandard Parser > 3.

### ExcelParser 
This parser is an attempt to fit the rejected tabular files. 

1. Segment (non-readme) sheets to blocks by a Breadth-First-Search. 
    - Post processed to find overlapping blocks and to sort the blocks
2. compute Stats for each block
3. For each block, Check if a block contains a table (Minimum 2 columns, should contain a considerate portion of numbers per row)
    a. If yes, extract headers and title
    b. Parse data 
    c. Construct df by matching data to respective headers

#### Upcoming:
1. complete the Features
    - Attaching Data Only Blocks to Header Only blocks
    - Merging Multi line Headers
2. Annotate num of tables per sheet 
3. Mass-Test the parser against compplete TU Corpus