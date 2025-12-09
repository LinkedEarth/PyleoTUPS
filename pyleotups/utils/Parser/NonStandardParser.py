# NonStandardParser.py
import re
import pandas as pd
import requests

# Import all models and utilities from the utils file
from .NonStandardParserUtils import (
    Block, LineInfo, BlockType,
    count_tokens, numeric_ratio, is_numeric, generate_row_pattern,
    _safe_mean, _safe_var, _safe_cv, _most_common,
    get_token_intervals_multi, merge_headers_by_overlap,
    generate_df, assign_tokens_by_overlap,
    refine_headers_by_correspondence
)


class NonStandardParser:
    """
    Parses non-standard, fixed-width, or misaligned text files (like
    those from NOAA) into a structured list of Blocks, each potentially
    containing a pandas DataFrame.

    The parser uses statistical heuristics to classify contiguous blocks
    of text and then applies different parsing strategies based on the
    classification.

    Attributes
    ----------
    file_path : str
        The local path or URL to the file being parsed.
    use_skip : bool
        Whether to skip to the "DATA:" descriptor in the file.
    lines : list[str]
        A list of all lines read from the file.
    blocks : list[Block]
        The final list of processed Block objects.

    Notes
    -----
    The parsing workflow is as follows:
    1.  A `NonStandardParser` instance is created with a `file_path`.
    2.  The public `parse()` method is called.
    3.  `_fetch_lines()` reads the file into `self.lines`.
    4.  `_segregate_blocks()` splits `self.lines` into `Block` objects
        (groups of non-empty lines) and saves them to `self.blocks`.
    5.  `parse()` iterates through each `block` in `self.blocks`.
    6.  `_process_block()` is called on each block, which:
        a.  Computes statistics for the block.
        b.  Classifies it (e.g., TABULAR, DATA, NARRATIVE).
        c.  Dispatches to a specific parsing method (e.g.,
            `_parse_tabular_block`).
    7.  The specific parse methods (e.g., `_parse_data_block`) handle
        logic for header borrowing, DataFrame generation, and error
        handling, modifying the `block` object in place.
    8.  `parse()` returns the fully processed `self.blocks` list.
    """

    def __init__(self, file_path, use_skip=True, use_refinement=True):
        """
        Initializes the parser.

        Parameters
        ----------
        file_path : str
            The local file path or HTTP/HTTPS URL of the text file to parse.
        use_skip : bool, optional
            If True (default), the parser will skip all lines until it finds
            a line starting with "DATA:". This is standard for NOAA files.
        use_refinement : bool, optional
            If True (default), attempts to refine column headers by analyzing the
            vertical alignment of data columns (histogramming).
            
            **Do's and Don'ts**:
            - **Do** set this to `True` for standard NOAA Paleo files.
            - **Don't** set this to `True` if your file has no "DATA:"
              marker, as it will raise a `ValueError`. Set it to `False`
              to parse the entire file from the beginning.
        """
        self.file_path = file_path
        self.use_skip = use_skip
        self.lines = []
        self.blocks = []
        self.use_refinement = use_refinement

    def parse(self):
        """
        Executes the full parsing workflow on the file.

        Returns
        -------
        list[Block]
            A list of processed Block objects. Each block may contain
            a DataFrame (`block.df`) if parsing was successful, or an
            error message (`block.error_message`) if it failed.

        Raises
        ------
        ValueError
            If `use_skip` is True and no "DATA:" line is found.
        requests.exceptions.RequestException
            If the `file_path` is a URL and it fails to fetch.
        """
        self._fetch_lines()

        start_idx = self._find_data_descriptor() if self.use_skip else -1
        if start_idx == -1 and self.use_skip:
            raise ValueError("No Data Descriptor found in the file.")

        self._segregate_blocks(start_idx + 1, len(self.lines))

        for idx, block in enumerate(self.blocks):
            self._process_block(block, idx)

        return self.blocks

    def _get_cv_for_delimiter(self, block, delimiter):
        """Returns the Coefficient of Variation for the given delimiter."""
        if not delimiter: return 1.0
        if "2," in delimiter: return block.stats.get("cv_multi", 1.0)
        if "\\t" in delimiter: return block.stats.get("cv_tab", 1.0)
        return block.stats.get("cv_single", 1.0)

    def _fetch_lines(self):
        """Fetches file content from path/URL and sets self.lines."""
        if self.file_path.lower().startswith('http'):
            response = requests.get(self.file_path)
            response.raise_for_status()
            self.lines = response.text.splitlines()
        else:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                text = f.read()
                self.lines = text.splitlines()

    def _find_data_descriptor(self):
        """Finds the 'DATA:' line index in self.lines."""
        return next((j for j, line in enumerate(self.lines)
                     if line.lower().startswith("data:")), -1)

    def _segregate_blocks(self, start=0, end=None):
        """
        Segregates self.lines into Blocks and pre-computes line stats.
        
        Splits the file into blocks based on empty lines. For each line,
        it also pre-computes token counts and numeric ratios for all
        potential delimiters.
        """
        blocks = []
        block = None
        block_idx = 0
        end = len(self.lines) if end is None else end

        for i in range(start, end):
            line = self.lines[i]
            if line.strip():
                if block is None:
                    block = Block(idx=block_idx, start=i, end=i)
                
                # Pre-compute stats for the line
                li = LineInfo(idx=i, text=line.rstrip("\n"))
                li.line_len = len(li.text)
                li.count_single_tokens = count_tokens(li.text, r"\s+")
                li.count_multispace_tokens = count_tokens(li.text, r"(\s{2,})")
                li.count_tab_tokens = count_tokens(li.text, r"\t")
                li.numeric_single_ratio = numeric_ratio(li.text, r"\s+")
                li.numeric_multispace_ratio = numeric_ratio(li.text, r"(\s{2,})")
                li.numeric_tab_ratio = numeric_ratio(li.text, r"\t")
                
                block.lines.append(li)
                block.end = i
            else:
                if block is not None:
                    blocks.append(block)
                    block_idx += 1
                    block = None

        if block is not None:
            blocks.append(block)

        self.blocks = blocks

    @staticmethod
    def _compute_statistics(block):
        """Computes and returns aggregate statistics for a block."""
        s, m, t, l, ns, nm, nt, lens = [], [], [], [], [], [], [], []
        for ln in block.lines:
            s.append(getattr(ln, "count_single_tokens", 0) or 0)
            m.append(getattr(ln, "count_multispace_tokens", 0) or 0)
            t.append(getattr(ln, "count_tab_tokens", 0) or 0)
            l.append(getattr(ln, "line_len", 0) or 0)
            ns.append(getattr(ln, "numeric_single_ratio", 0.0) or 0.0)
            nm.append(getattr(ln, "numeric_multispace_ratio", 0.0) or 0.0)
            nt.append(getattr(ln, "numeric_tab_ratio", 0.0) or 0.0)
            lens.append(getattr(ln, "line_len",
                                len(getattr(ln, "text", "") or "")))

        mean_single = _safe_mean(s)
        mean_multi = _safe_mean(m)
        mean_tab = _safe_mean(t)
        mean_line_len = _safe_mean(l) # Changed from 'l'

        # Bug fix: Corrected last key from 'cv_tab' to 'cv_line'
        stats = {
            "mean_single": mean_single,
            "cv_single": _safe_cv(s, mean_single),
            "mode_single": _most_common(s),
            "mean_multi": mean_multi,
            "cv_multi": _safe_cv(m, mean_multi),
            "mode_multi": _most_common(m),
            "max_multi": max(m) if m else 0,
            "mean_tab": mean_tab,
            "cv_tab": _safe_cv(t, mean_tab),
            "mode_tab": _most_common(t),
            "mean_numeric_single": _safe_mean(ns),
            "mean_numeric_multi": _safe_mean(nm),
            "mean_numeric_tab": _safe_mean(nt),
            "n_lines": len(block.lines),
            "n_nonempty": sum(1 for ln in block.lines if
                              (getattr(ln, "text", "") or "").strip()),
            "mean_line_len": _safe_mean(lens), # Use 'lens' for mean
            "cv_line": _safe_cv(lens, _safe_mean(lens)), # Use 'lens'
        }
        return stats

    @staticmethod
    def _choose_delimiter(block, strict=False):
        """
        Chooses the best delimiter for a block based on its stats.

        Parameters
        ----------
        block : Block
            The block to analyze (must have `block.stats` computed).
        strict : bool, optional
            If True, only returns a delimiter if its token count is
            perfectly consistent (CV=0) and it has > 1 token.
            If False, returns the "best guess" delimiter (tab or
            multi-space) with the lowest CV, even if imperfect.
            
        Returns
        -------
        str or None
            The regex string of the best delimiter, or None if no
            suitable delimiter is found.
        """
        candidates = [("tab", r"\t"), ("multi", r"(\s{2,})"), ("single", r"\s+")]
        if strict:
            for k, pattern in candidates:
                cv = block.stats.get(f"cv_{k}", 1)
                mode = block.stats.get(f"mode_{k}", 0)
                if cv == 0 and mode > 1:
                    return pattern
            return None  # No "perfect" delimiter found

        # Non-strict mode: find best "imperfect" delimiter
        best_pattern, best_cv = None, float('inf')
        # Only check tab and multi-space for non-strict
        for k, pattern in candidates[:2]:
            cv = block.stats.get(f"cv_{k}", 1)
            mode = block.stats.get(f"mode_{k}", 0)
            if mode > 1 and cv < best_cv:
                best_cv = cv
                best_pattern = pattern
        return best_pattern

    @staticmethod
    def _detect_header_extent(block, delimiter):
        """Detects the number of header lines (extent) in a block."""
        patterns, title_line = [], None
        for i, line in enumerate(block.lines):
            tokens = [t for t in re.split(delimiter, line.text.strip()) if t.strip()]
            pattern = generate_row_pattern(tokens)
            patterns.append(pattern)
            if i == 0 and pattern == "S":  # Check for title line
                title_line = i

        start_i = title_line + 1 if title_line is not None else 0
        extent = 0
        for i, pattern in enumerate(patterns[start_i:]):
            is_all_s = all(c == "S" for c in pattern)
            is_all_n = all(c == "N" for c in pattern)
            next_is_all_s = (start_i + i + 1 < len(patterns)) and all(
                c == "S" for c in patterns[start_i + i + 1]
            )
            # A header line is all-string, or all-numeric *if*
            # it's the first line and the next line is all-string.
            if is_all_s or (i == 0 and is_all_n and next_is_all_s):
                extent += 1
            else:
                break
        return extent, title_line

    @staticmethod
    def _detect_header_indices(block, delimiter):
        """
        Scans the block to identify the indices of the Title and Header lines.
        
        Returns
        -------
        title_idx : int
            The index of the title line, or -1 if not found.
        header_start : int
            The inclusive start index of the header rows.
        header_end : int
            The exclusive end index of the header rows.
        """
        patterns = []
        for line in block.lines:
            tokens = [t for t in re.split(delimiter, line.text.strip()) if t.strip()]
            patterns.append(generate_row_pattern(tokens))

        # 1. Scan for the "Candidate Block" (consecutive String or Unit lines)
        candidate_end = 0
        for i, pattern in enumerate(patterns):
            curr_idx = i
            is_all_s = all(c == "S" for c in pattern)
            
            # Check for Unit Line (current is string, next is mostly numeric)
            is_unit_line = False
            if curr_idx + 1 < len(patterns):
                next_pattern = patterns[curr_idx + 1]
                if len(next_pattern) > 0:
                    next_numeric_ratio = next_pattern.count("N") / len(next_pattern)
                    curr_numeric_ratio = pattern.count("N") / len(pattern) if pattern else 0
                    if next_numeric_ratio > 0.5 and curr_numeric_ratio < 0.5:
                        is_unit_line = True

            # Special case: Numeric Year as Title (e.g. "2023")
            is_first_numeric_title = (
                i == 0 and all(c == "N" for c in pattern) and 
                (curr_idx + 1 < len(patterns) and all(c == "S" for c in patterns[curr_idx + 1]))
            )

            if is_all_s or is_unit_line or is_first_numeric_title:
                candidate_end += 1
            else:
                break
        
        # 2. Resolve Title vs. Headers based on Geometry
        title_idx = -1
        header_start = 0
        header_end = candidate_end

        if candidate_end == 0:
            return -1, 0, 0

        def get_token_count(idx):
            if idx < 0 or idx >= len(block.lines): return 0
            return len([t for t in re.split(delimiter, block.lines[idx].text.strip()) if t.strip()])

        # Check A: Interposed Title (Title at the BOTTOM of header block)
        if candidate_end > 1:
            last_idx = candidate_end - 1
            prev_idx = last_idx - 1
            if get_token_count(last_idx) == 1 and get_token_count(prev_idx) > 1:
                title_idx = last_idx
                header_end = last_idx # Exclude last line from headers
                return title_idx, header_start, header_end

        # Check B: Top Title (Title at the TOP)
        if candidate_end > 0:
            if get_token_count(0) == 1:
                if candidate_end > 1 and get_token_count(1) > 1:
                     title_idx = 0
                     header_start = 1
                elif candidate_end == 1:
                    # Single line title, no headers
                    title_idx = 0
                    header_start = 1
                    header_end = 1
        
        return title_idx, header_start, header_end

    
    @staticmethod
    def _extract_headers(block, delimiter):
        """Extracts header names and intervals using normalized indices."""
        title_idx, h_start, h_end = NonStandardParser._detect_header_indices(block, delimiter)

        # 1. Handle Title
        if title_idx != -1:
            block.title = block.lines[title_idx].text
            
        # 2. Slice Headers
        header_lines = block.lines[h_start : h_end]
        
        # 3. Calculate Data Offset (header_extent)
        # Data starts after the MAX of header_end or (Title + 1)
        structure_end = h_end
        if title_idx != -1:
            structure_end = max(structure_end, title_idx + 1)
        block.header_extent = structure_end

        # 4. Process Headers
        if not header_lines:
            return [], block.header_extent
            
        token_maps = [get_token_intervals_multi(line.text, delimiter)
                      for line in header_lines]
        
        if len(header_lines) == 1:
            token_objs = get_token_intervals_multi(header_lines[0].text, delimiter)
            return [{"name": t["display"], "interval": t["interval"]}
                    for t in token_objs], block.header_extent

        return merge_headers_by_overlap(token_maps), block.header_extent

    @staticmethod
    def _classify_block(block):
        """Classifies a block into a BlockType based on its stats."""
        stats = block.stats
        if stats["n_lines"] < 2:
            return BlockType.NARRATIVE
            
        if stats["mean_numeric_single"] < 0.25:
            # Stricter Narrative Check 2: Max tokens is 1 (Pure Narrative)
            if stats["max_multi"] == 1: 
                return BlockType.NARRATIVE
            elif (stats["mode_multi"] > 1 or stats["mode_tab"] > 1) and stats["n_lines"] <= 6:
                return BlockType.HEADER_ONLY

        best_delimiter = NonStandardParser._choose_delimiter(block, strict=True)
        if not best_delimiter:
            # Fallback only if block is substantial, else Narrative
            return BlockType.TABULAR if len(block.lines) > 2 else BlockType.NARRATIVE

        headers, extent = NonStandardParser._extract_headers(block, best_delimiter)

        if headers:
            block.headers = headers
            block.header_extent = extent
            block.delimiter = best_delimiter
            if extent < stats["n_lines"]:
                return BlockType.COMPLETE_TABULAR
            else:
                return BlockType.HEADER_ONLY
        else:
            block.delimiter = best_delimiter
            return BlockType.DATA

    def _process_block(self, block, current_idx):
        """Main dispatcher for parsing logic based on block type."""
        block.stats = self._compute_statistics(block)
        block_type = self._classify_block(block)
        block.block_type = block_type

        # Dispatch to the correct parsing method
        if block_type == BlockType.COMPLETE_TABULAR:
            self._parse_complete_tabular_block(block)
        elif block_type == BlockType.TABULAR:
            self._parse_tabular_block(block, current_idx)
        elif block_type == BlockType.HEADER_ONLY:
            self._parse_header_block(block)
        elif block_type == BlockType.DATA:
            self._parse_data_block(block, current_idx)
        
        # NARRATIVE blocks are left as-is
        # ERROR blocks (if classified early) are also left as-is

    def _find_previous_header_block(self, current_idx):
        """Finds the most recent valid header-providing block."""
        for i in range(current_idx - 1, -1, -1):
            prev_block = self.blocks[i]
            # Only borrow from "clean" header or table blocks
            if prev_block.block_type in (BlockType.HEADER_ONLY,
                                         BlockType.COMPLETE_TABULAR, BlockType.TABULAR):
                if prev_block.headers and prev_block.delimiter:
                    return prev_block
        return None

    def _parse_complete_tabular_block(self, block):
        """Parses a "perfect" (CV=0) tabular block."""
        try:
            df = generate_df(
                block.lines,
                block.delimiter,
                block.headers,
                block.header_extent
            )
            block.df = df
        except Exception as e:
            block.block_type = BlockType.ERROR
            block.error_message = f"Failed to parse complete tabular block: {e}"
            block.df = None

    def _parse_tabular_block(self, block, current_idx):
        """Parses an imperfect tabular block, attempting refinement."""
        try:
            if not block.delimiter:
                block.delimiter = self._choose_delimiter(block, strict=False)
            if not block.delimiter: 
                raise ValueError("Could not determine a suitable delimiter.")

            if not block.headers:
                headers, extent = self._extract_headers(block, block.delimiter)
                block.headers = headers
                # header_extent is updated inside _extract_headers

            # --- Refinement Step ---
            if self.use_refinement and block.header_extent > 0:
                # Re-detect indices to get clean header lines (excluding Title)
                title_idx, h_start, h_end = self._detect_header_indices(block, block.delimiter)
                if h_end > h_start:
                    header_lines_subset = block.lines[h_start:h_end]
                    data_lines = block.lines[block.header_extent:]
                    
                    refined = refine_headers_by_correspondence(
                        header_lines_subset, data_lines, block.delimiter
                    )
                    if refined:
                        block.headers = refined

            if not block.headers:
                prev = self._find_previous_header_block(current_idx)
                if not prev: 
                    raise ValueError("No headers found in block or preceding blocks.")
                
                borrowed = prev.headers
                if self.use_refinement and prev.header_extent > 0:
                    p_title, p_start, p_end = self._detect_header_indices(prev, prev.delimiter)
                    if p_end > p_start:
                        header_lines_subset = prev.lines[p_start:p_end]
                        data_lines = block.lines # Use CURRENT data for refinement
                        refined = refine_headers_by_correspondence(
                            header_lines_subset, data_lines, prev.delimiter
                        )
                        if refined:
                            borrowed = refined
                
                block.headers = borrowed
                block.delimiter = prev.delimiter
                # Ensure extent is set if it wasn't already (e.g. if we had a Title but no headers)
                if block.header_extent is None:
                    block.header_extent = 0
                prev.used_as_header_for.append(block.idx)

            cv = self._get_cv_for_delimiter(block, block.delimiter)
            
            try:
                if cv > 0:
                    raise ValueError("Detected jagged columns (CV > 0).")    
                df = generate_df(block.lines, block.delimiter, block.headers, block.header_extent)
            except ValueError:
                df = assign_tokens_by_overlap(block.lines, block.delimiter, block.headers, block.header_extent)
            
            block.df = df

        except Exception as e:
            block.block_type = BlockType.ERROR
            block.error_message = f"Failed to parse tabular block: {e}"
            block.df = None

    def _parse_header_block(self, block):
        """Ensures headers are extracted for HEADER_ONLY blocks."""
        try:
            if not block.delimiter:
                block.delimiter = self._choose_delimiter(block, strict=False)

            if not block.headers and block.delimiter:
                headers, extent = self._extract_headers(block, block.delimiter)
                block.headers = headers
                block.header_extent = extent

            if not block.headers:
                raise ValueError("Could not extract headers from header-only block.")
        except Exception as e:
            block.block_type = BlockType.ERROR
            block.error_message = f"Failed to parse header block: {e}"

    def _parse_data_block(self, block, current_idx):
        """Parses a DATA block by borrowing headers, respecting local Titles."""
        try:
            prev = self._find_previous_header_block(current_idx)
            if not prev: 
                raise ValueError("No preceding headers found for this data block.")
            
            borrowed = prev.headers
            
            # --- Refinement Step ---
            if self.use_refinement and prev.header_extent > 0:
                p_title, p_start, p_end = self._detect_header_indices(prev, prev.delimiter)
                if p_end > p_start:
                    header_lines_subset = prev.lines[p_start:p_end]
                    data_lines = block.lines 
                    refined = refine_headers_by_correspondence(
                        header_lines_subset, data_lines, prev.delimiter
                    )
                    if refined:
                        borrowed = refined

            block.headers = borrowed
            block.delimiter = prev.delimiter

            
            try:
                df = generate_df(block.lines, block.delimiter, block.headers, block.header_extent)
            except ValueError:
                df = assign_tokens_by_overlap(block.lines, block.delimiter, block.headers, block.header_extent)
            
            block.df = df
            prev.used_as_header_for.append(block.idx)

        except Exception as e:
            block.block_type = BlockType.ERROR
            block.error_message = f"Failed to parse data block: {e}"
            block.df = None
