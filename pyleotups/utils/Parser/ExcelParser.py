import io
import os
import math
import requests
import pandas as pd
from dataclasses import dataclass, field
from typing import Any, List, Optional, Tuple, Iterable, Dict
from enum import Enum


NUMERIC_THRESHOLD_HEADER = 0.25
MAX_TABLE_COLUMN_LEN = 2
MIN_REGION_AREA = 1


class BlockType(str, Enum):
    """Enumeration for the different types a Block can be classified as."""
    NARRATIVE = "narrative"
    HEADER_ONLY = "header-only"
    DATA_ONLY = "data-only"
    COMPLETE_TABULAR = "complete-tabular"
    METADATA = "metadata"
    EMPTY = "empty"


@dataclass
class SheetGrid:
    """
    Lightweight 0-based grid wrapper over a sheet's values.

    Attributes
    ----------
    name : str
        Name of the Excel sheet.
    sheet_idx : int
        Index of the sheet in the workbook.
    nrows : int
        Number of used rows.
    ncols : int
        Number of used columns.
    _values : List[List[Any]]
        The raw cell values (row-major).
    merged_spans : List[Tuple[int, int, int, int]], optional
        List of merged cell ranges (r0, r1, c0, c1) inclusive 0-based.
    """
    name: str
    sheet_idx: int
    nrows: int
    ncols: int
    _values: List[List[Any]]
    merged_spans: List[Tuple[int, int, int, int]] = field(default_factory=list)

    def get_value(self, r: int, c: int) -> Any:
        """Safe accessor for grid values."""
        if 0 <= r < self.nrows and 0 <= c < self.ncols:
            return self._values[r][c]
        return None

    def is_empty(self, r: int, c: int) -> bool:
        """Checks if a cell is visually empty (None or whitespace string)."""
        v = self.get_value(r, c)
        if v is None:
            return True
        if isinstance(v, str):
            return len(v.strip()) == 0
        return False


@dataclass
class Block:
    """
    Represents a contiguous non-empty region in an Excel sheet.

    Attributes
    ----------
    idx : int
        Unique identifier for the block in the parsing session.
    sheet_name : str
        Name of the sheet containing this block.
    sheet_idx : int
        Index of the sheet.
    top : int
        Top row index (inclusive).
    bottom : int
        Bottom row index (inclusive).
    left : int
        Left column index (inclusive).
    right : int
        Right column index (inclusive).
    area : int
        Number of cells in the block.
    block_type : BlockType
        Classified type of the block (e.g., TABULAR, NARRATIVE).
    headers : List[str]
        List of column headers strings.
    header_extent : int
        Number of rows detected as headers.
    title : str
        Detected title text for the block.
    stats : dict
        Computed statistics (numeric ratios, dimensions, etc.).
    df : pd.DataFrame
        The parsed DataFrame (if successful).
    """
    idx: int = -1
    sheet_name: str = ""
    sheet_idx: int = 0
    top: int = 0
    bottom: int = 0
    left: int = 0
    right: int = 0
    area: int = 0

    block_type: BlockType = BlockType.NARRATIVE
    headers: Optional[List[str]] = None
    header_extent: int = 0
    title: Optional[str] = None
    stats: dict = field(default_factory=dict)
    df: Optional[pd.DataFrame] = None
    
    # Internal usage for parser logic
    delimiter: str = "excel" 


# ---------------------------------------------------------------------
# ExcelParser Class
# ---------------------------------------------------------------------

class ExcelParser:
    """
    Parses Excel files by detecting contiguous blocks of non-empty cells
    and converting them into structured DataFrames.

    It handles:
    - Spatial segmentation (BFS) to find tables.
    - Merged cell propagation.
    - Statistical header detection.
    - Multi-row header merging.
    - Header borrowing for data-only blocks.

    Attributes
    ----------
    file_path : str
        The local path or URL to the Excel file.
    sheets : List[SheetGrid]
        The loaded sheets content.
    blocks : List[Block]
        The segregated and processed blocks.
    _header_registry : Dict[str, List[Block]]
        Internal registry for header borrowing logic.
    """

    def __init__(self, file_path: str):
        """
        Initializes the ExcelParser.

        Parameters
        ----------
        file_path : str
            The local file path or HTTP/HTTPS URL of the .xls/.xlsx file.
        """
        self.file_path = file_path
        self.sheets: List[SheetGrid] = []
        self.blocks: List[Block] = []
        self._header_registry: Dict[str, List[Block]] = {}

    def parse(self) -> List[Block]:
        """
        Executes the full parsing workflow.

        Returns
        -------
        List[Block]
            A list of processed Block objects, potentially containing DataFrames.
        """
        self._fetch_workbook()
        self._segregate_blocks()

        for idx, block in enumerate(self.blocks):
            self._process_block(block, idx)

        return self.blocks

    def _fetch_workbook(self):
        """
        Fetches file content and populates self.sheets using openpyxl or xlrd.
        """
        # 1. Determine source bytes or path
        content = None
        is_path = False
        
        if self.file_path.lower().startswith("http"):
            response = requests.get(self.file_path)
            response.raise_for_status()
            content = io.BytesIO(response.content)
        else:
            if os.path.exists(self.file_path):
                content = self.file_path
                is_path = True
            else:
                raise FileNotFoundError(f"File not found: {self.file_path}")

        # 2. Open Workbook
        wb_xlsx, wb_xls = None, None
        
        # Try openpyxl (.xlsx) first
        try:
            from openpyxl import load_workbook
            # data_only=True gets values, not formulas
            wb_xlsx = load_workbook(filename=content, data_only=True, read_only=False)
        except Exception:
            # Fallback to xlrd (.xls)
            if not is_path and hasattr(content, 'seek'):
                content.seek(0)
            
            try:
                import xlrd
                if is_path:
                    wb_xls = xlrd.open_workbook(content, formatting_info=True)
                else:
                    wb_xls = xlrd.open_workbook(file_contents=content.read(), formatting_info=True)
            except Exception as e:
                raise RuntimeError(
                    f"Failed to open workbook. Ensure 'openpyxl' (xlsx) or 'xlrd' (xls) is installed. Error: {e}"
                )

        # 3. Convert to SheetGrid objects
        self.sheets = []
        if wb_xlsx:
            self._parse_openpyxl(wb_xlsx)
        elif wb_xls:
            self._parse_xlrd(wb_xls)

    def _parse_openpyxl(self, wb):
        """Internal helper to convert openpyxl workbook to SheetGrids."""
        for idx, ws in enumerate(wb.worksheets):
            title = (ws.title or "").lower()
            if "readme" in title:
                continue

            max_row = ws.max_row or 0
            max_col = ws.max_column or 0
            values = [[cell.value for cell in row] for row in ws.iter_rows(min_row=1, max_row=max_row,
                                                                          min_col=1, max_col=max_col)]
            merged_spans = []
            
            # Propagate merged cells
            for rng in ws.merged_cells.ranges:
                r0, r1 = rng.min_row - 1, rng.max_row - 1
                c0, c1 = rng.min_col - 1, rng.max_col - 1
                merged_spans.append((r0, r1, c0, c1))
                
                tl = values[r0][c0] if 0 <= r0 < max_row and 0 <= c0 < max_col else None
                if tl is not None and (isinstance(tl, str) and tl.strip() == ""):
                    tl = None
                
                if tl is not None:
                    for rr in range(r0, r1 + 1):
                        for cc in range(c0, c1 + 1):
                            current = values[rr][cc]
                            if current is None or (isinstance(current, str) and current.strip() == ""):
                                values[rr][cc] = tl

            nrows_used, ncols_used = self._compute_used_bounds(values)
            trimmed = [row[:ncols_used] for row in values[:nrows_used]]
            
            self.sheets.append(SheetGrid(ws.title, idx, nrows_used, ncols_used, trimmed, merged_spans))

    def _parse_xlrd(self, wb):
        """Internal helper to convert xlrd workbook to SheetGrids."""
        for idx in range(wb.nsheets):
            sh = wb.sheet_by_index(idx)
            title = (sh.name or "").lower()
            if "readme" in title:
                continue

            nrows, ncols = sh.nrows, sh.ncols
            values = [[sh.cell_value(r, c) for c in range(ncols)] for r in range(nrows)]
            merged_spans = []

            if hasattr(sh, "merged_cells"):
                for rlo, rhi, clo, chi in sh.merged_cells:
                    r0, r1 = rlo, rhi - 1
                    c0, c1 = clo, chi - 1
                    merged_spans.append((r0, r1, c0, c1))
                    
                    tl = values[r0][c0] if 0 <= r0 < nrows and 0 <= c0 < ncols else None
                    if tl is not None and (isinstance(tl, str) and tl.strip() == ""):
                        tl = None
                    if tl is not None:
                        for rr in range(r0, r1 + 1):
                            for cc in range(c0, c1 + 1):
                                current = values[rr][cc]
                                if current is None or (isinstance(current, str) and current.strip() == ""):
                                    values[rr][cc] = tl

            nrows_used, ncols_used = self._compute_used_bounds(values)
            trimmed = [row[:ncols_used] for row in values[:nrows_used]]
            
            self.sheets.append(SheetGrid(sh.name, idx, nrows_used, ncols_used, trimmed, merged_spans))

    def _segregate_blocks(self):
        """
        Identify contiguous blocks of non-empty cells in all sheets.
        Applies BFS spatial clustering and minimal enclosure consolidation.
        """
        raw_blocks = []

        for grid in self.sheets:
            visited = [[False] * max(1, grid.ncols) for _ in range(max(1, grid.nrows))]
            
            for r in range(grid.nrows):
                for c in range(grid.ncols):
                    if visited[r][c] or grid.is_empty(r, c):
                        visited[r][c] = True
                        continue

                    # BFS to find island
                    queue = [(r, c)]
                    visited[r][c] = True
                    top = bottom = r
                    left = right = c
                    area = 0

                    while queue:
                        rr, cc = queue.pop()
                        if grid.is_empty(rr, cc):
                            continue
                        area += 1
                        top = min(top, rr)
                        bottom = max(bottom, rr)
                        left = min(left, cc)
                        right = max(right, cc)

                        for dr, dc in self._neighbors8():
                            nr, nc = rr + dr, cc + dc
                            if 0 <= nr < grid.nrows and 0 <= nc < grid.ncols:
                                if not visited[nr][nc]:
                                    visited[nr][nc] = True
                                    if not grid.is_empty(nr, nc):
                                        queue.append((nr, nc))

                    if area >= MIN_REGION_AREA:
                        raw_blocks.append(Block(
                            idx=-1,
                            sheet_name=grid.name,
                            sheet_idx=grid.sheet_idx,
                            top=top, bottom=bottom, left=left, right=right,
                            area=area
                        ))

        # Consolidate Enclosures & Sort
        consolidated = self._consolidate_enclosures_minimal(raw_blocks)
        
        # Sort: Sheet, then visual reading order (top-left)
        consolidated.sort(key=lambda b: (b.sheet_idx, b.left, b.top, b.right, -b.area))
        
        # Assign IDs
        for i, blk in enumerate(consolidated):
            blk.idx = i
            
        self.blocks = consolidated

    def _process_block(self, block: Block, idx: int):
        """
        Main processing logic for a single block.
        
        Computes statistics, detects headers, classifies the block,
        and generates the DataFrame if applicable.
        """
        grid = self._get_sheet_by_name(block.sheet_name)
        if not grid:
            return

        # 0. Gate: Minimum distinct columns check
        col_len = self._block_column_len(block, grid)
        if col_len < MAX_TABLE_COLUMN_LEN:
            block.block_type = BlockType.NARRATIVE
            return

        # 1. Compute Profiles (Stats)
        block.stats = self._compute_statistics(block, grid)

        # 2. Detect Header Extent & Title
        hdr_info = self._detect_header_extent(block, grid, block.stats)
        block.header_extent = hdr_info.get("header_extent", 0)
        block.title = self._extract_title_text(block, grid, hdr_info)

        # 3. Extract & Merge Headers
        merged_headers = self._extract_and_merge_headers(block, grid, hdr_info)
        block.headers = merged_headers

        # 4. Classify
        block.block_type = self._classify_block(block, block.stats, merged_headers)

        # 5. Register Header-Bearing Blocks (for borrowing)
        if block.block_type in (BlockType.COMPLETE_TABULAR, BlockType.HEADER_ONLY) and merged_headers:
            self._header_registry.setdefault(block.sheet_name, []).append(block)

        # 6. Parse Data / Handle Borrowing
        if block.block_type == BlockType.DATA_ONLY:
            # Attempt to borrow headers from a previous block on the same sheet
            candidates = self._header_registry.get(block.sheet_name, [])
            donor = self._find_borrow_candidate(block, candidates)
            
            if donor and donor.headers:
                borrowed = self._borrow_headers(block, donor)
                block.headers = borrowed
                block.block_type = BlockType.COMPLETE_TABULAR
                
                # Adjust header info for generation (data starts immediately after title)
                hdr_info_borrow = dict(hdr_info)
                hdr_info_borrow["header_extent"] = 0 
                block.df = self._generate_df(block, grid, borrowed, hdr_info_borrow)
            else:
                block.df = None
        
        elif block.block_type == BlockType.COMPLETE_TABULAR and merged_headers:
            block.df = self._generate_df(block, grid, merged_headers, hdr_info)
        
        else:
            block.df = None

    # -----------------------------------------------------------------
    # Core Logic Methods
    # -----------------------------------------------------------------

    def _compute_statistics(self, block: Block, grid: SheetGrid) -> dict:
        """Computes numeric density and non-empty counts for rows/cols."""
        row_nonempty, row_numeric = [], []
        
        for r in range(block.top, block.bottom + 1):
            vals = [grid.get_value(r, c) for c in range(block.left, block.right + 1)]
            nonempties = [v for v in vals if not self._is_val_empty(v)]
            k = len(nonempties)
            row_nonempty.append(k)
            
            if k == 0:
                row_numeric.append(0.0)
            else:
                ratio = sum(1 for v in nonempties if self._is_numeric_cell(v)) / k
                row_numeric.append(ratio)

        # Basic aggregate stats
        mean_nonempty = sum(row_nonempty) / len(row_nonempty) if row_nonempty else 0
        mean_numeric = sum(row_numeric) / len(row_numeric) if row_numeric else 0

        return {
            "row_nonempty_counts": row_nonempty,
            "row_numeric_ratio": row_numeric,
            "mean_row_nonempty": mean_nonempty,
            "mean_row_numeric_ratio": mean_numeric
        }

    def _detect_header_extent(self, block: Block, grid: SheetGrid, stats: dict) -> dict:
        """
        Determines the title row and how many lines constitute the header.
        
        Title Rule: First row of block has exactly 1 non-empty cell.
        Header Rule: Extend while row numeric ratio < threshold.
        """
        row_nonempty = stats["row_nonempty_counts"]
        row_numeric = stats["row_numeric_ratio"]
        
        title_row_idx = None
        # Check if first row looks like a title (single cell at top-left of block)
        if row_nonempty and row_nonempty[0] == 1:
            if not grid.is_empty(block.top, block.left):
                title_row_idx = block.top

        start_offset = 1 if title_row_idx is not None else 0
        header_extent = 0
        
        for i in range(start_offset, len(row_numeric)):
            if row_numeric[i] < NUMERIC_THRESHOLD_HEADER:
                header_extent += 1
            else:
                break
                
        return {"title_row": title_row_idx, "header_extent": header_extent}

    def _extract_title_text(self, block: Block, grid: SheetGrid, hdr_info: dict) -> Optional[str]:
        tr = hdr_info.get("title_row")
        if tr is None:
            return None
        # Find the single non-empty cell
        for c in range(block.left, block.right + 1):
            val = grid.get_value(tr, c)
            if not self._is_val_empty(val):
                return str(val).strip()
        return None

    def _extract_and_merge_headers(self, block: Block, grid: SheetGrid, hdr_info: dict) -> Optional[List[str]]:
        """
        Extracts multi-row headers, respecting merged cells, and merges them vertically.
        """
        H = hdr_info.get("header_extent", 0)
        if H <= 0:
            return None

        # 1. Build lookup for merged cells
        merged_idx = {}
        for (r0, r1, c0, c1) in grid.merged_spans:
            for r in range(r0, r1 + 1):
                for c in range(c0, c1 + 1):
                    merged_idx[(r, c)] = (r0, r1, c0, c1)

        # 2. Extract spans for each header row
        title_row = hdr_info.get("title_row")
        start_row = block.top + (1 if title_row is not None else 0)
        header_rows_spans = []

        for r in range(start_row, start_row + H):
            spans = []
            c = block.left
            while c <= block.right:
                txt = self._norm_text(grid.get_value(r, c))
                if not txt:
                    c += 1
                    continue
                
                # Determine horizontal span of this text
                if (r, c) in merged_idx:
                    # Use merged cell info
                    _, _, c0, c1 = merged_idx[(r, c)]
                    # Clip to block bounds
                    c0, c1 = max(c0, block.left), min(c1, block.right)
                    if c == c0: # Add only once
                        spans.append({"c0": c0, "c1": c1, "text": txt})
                    c = c1 + 1
                else:
                    # Collapse repeats if not strictly merged
                    c0, c1 = c, c
                    while c1 + 1 <= block.right and self._norm_text(grid.get_value(r, c1 + 1)) == txt:
                        c1 += 1
                    spans.append({"c0": c0, "c1": c1, "text": txt})
                    c = c1 + 1
            header_rows_spans.append(spans)

        # 3. Merge vertically down columns
        width = block.right - block.left + 1
        col_tokens = [[] for _ in range(width)]

        for row_spans in header_rows_spans:
            for j in range(block.left, block.right + 1):
                # Check which span covers column j
                token = ""
                for sp in row_spans:
                    if sp["c0"] <= j <= sp["c1"]:
                        token = sp["text"]
                        break
                if token:
                    col_tokens[j - block.left].append(token)

        # 4. Join and Dedupe
        headers = []
        for tokens in col_tokens:
            cleaned = []
            prev = None
            for t in tokens:
                t = t.strip()
                if t and t != prev:
                    cleaned.append(t)
                    prev = t
            headers.append(" ".join(cleaned))

        # 5. Ensure uniqueness
        return self._ensure_unique(headers)

    def _classify_block(self, block: Block, stats: dict, headers: Optional[List[str]]) -> BlockType:
        """Determines BlockType based on header existence and data presence."""
        H = block.header_extent
        # Data starts after title + header
        offset = (1 if block.title else 0) + H
        data_start_row_idx = offset  # relative to stats array 0-index

        # Count data rows (rows with >= 1 non-empty cell)
        data_rows = 0
        counts = stats["row_nonempty_counts"]
        if data_start_row_idx < len(counts):
            for i in range(data_start_row_idx, len(counts)):
                if counts[i] > 0:
                    data_rows += 1

        if headers and data_rows > 0:
            return BlockType.COMPLETE_TABULAR
        if headers and data_rows == 0:
            return BlockType.HEADER_ONLY
        if not headers and data_rows > 0:
            return BlockType.DATA_ONLY
        
        return BlockType.NARRATIVE

    def _generate_df(self, block: Block, grid: SheetGrid, headers: List[str], hdr_info: dict) -> pd.DataFrame:
        """Creates DataFrame from the block's data region."""
        H = hdr_info.get("header_extent", 0)
        tr = hdr_info.get("title_row")
        data_start = block.top + (1 if tr is not None else 0) + H
        
        rows = []
        ncols = len(headers)
        
        for r in range(data_start, block.bottom + 1):
            raw_vals = [grid.get_value(r, c) for c in range(block.left, block.right + 1)]
            
            # Skip completely empty rows
            if all(self._is_val_empty(v) for v in raw_vals):
                continue
                
            # Normalize length
            if len(raw_vals) < ncols:
                raw_vals += [None] * (ncols - len(raw_vals))
            elif len(raw_vals) > ncols:
                raw_vals = raw_vals[:ncols]
            
            rows.append(raw_vals)
            
        return pd.DataFrame(rows, columns=headers)

    # -----------------------------------------------------------------
    # Helpers: Borrowing & Utils
    # -----------------------------------------------------------------

    def _find_borrow_candidate(self, block: Block, candidates: List[Block]) -> Optional[Block]:
        """Finds a preceding header block with exact horizontal alignment."""
        for cand in reversed(candidates):
            if cand.left == block.left and cand.right == block.right:
                return cand
        return None

    def _borrow_headers(self, block: Block, donor: Block) -> List[str]:
        """Copies headers from donor, adjusting length if necessary."""
        width = block.right - block.left + 1
        hdrs = list(donor.headers)
        if len(hdrs) < width:
            hdrs += [f"unnamed_{i}" for i in range(len(hdrs), width)]
        elif len(hdrs) > width:
            hdrs = hdrs[:width]
        return hdrs

    def _get_sheet_by_name(self, name: str) -> Optional[SheetGrid]:
        for s in self.sheets:
            if s.name == name:
                return s
        return None

    def _block_column_len(self, block: Block, grid: SheetGrid) -> int:
        """Counts columns that have at least one non-empty cell."""
        count = 0
        for c in range(block.left, block.right + 1):
            for r in range(block.top, block.bottom + 1):
                if not grid.is_empty(r, c):
                    count += 1
                    break
        return count

    # -----------------------------------------------------------------
    # Static Utils
    # -----------------------------------------------------------------

    @staticmethod
    def _neighbors8() -> Iterable[Tuple[int, int]]:
        return [(-1, -1), (-1, 0), (-1, 1),
                (0, -1),           (0, 1),
                (1, -1),  (1, 0),  (1, 1)]

    @staticmethod
    def _compute_used_bounds(values: List[List[Any]]) -> Tuple[int, int]:
        """Trims trailing empty rows and columns."""
        if not values:
            return 0, 0
        nrows = len(values)
        ncols = max((len(row) for row in values), default=0)

        # Pad rows
        for row in values:
            if len(row) < ncols:
                row += [None] * (ncols - len(row))

        def is_empty(v):
            return v is None or (isinstance(v, str) and not v.strip())

        last_row = -1
        for r in range(nrows - 1, -1, -1):
            if any(not is_empty(values[r][c]) for c in range(ncols)):
                last_row = r
                break
        
        if last_row < 0:
            return 0, 0

        last_col = -1
        for c in range(ncols - 1, -1, -1):
            if any(not is_empty(values[r][c]) for r in range(last_row + 1)):
                last_col = c
                break
        
        return last_row + 1, last_col + 1

    @staticmethod
    def _consolidate_enclosures_minimal(blocks: List[Block]) -> List[Block]:
        """Removes blocks fully enclosed within larger blocks on the same sheet."""
        by_sheet = {}
        for b in blocks:
            by_sheet.setdefault(b.sheet_idx, []).append(b)

        survivors = []
        for _, sheet_blocks in by_sheet.items():
            # Sort by area descending
            sorted_blocks = sorted(sheet_blocks, key=lambda b: b.area, reverse=True)
            dropped = set()
            
            for i, A in enumerate(sorted_blocks):
                if i in dropped: continue
                for j, B in enumerate(sorted_blocks):
                    if i == j or j in dropped: continue
                    # Check if B inside A
                    if (A.left <= B.left and B.right <= A.right and 
                        A.top <= B.top and B.bottom <= A.bottom):
                        dropped.add(j)
            
            survivors.extend([b for k, b in enumerate(sorted_blocks) if k not in dropped])
        return survivors

    @staticmethod
    def _is_numeric_cell(val: Any) -> bool:
        if val is None: return False
        if isinstance(val, (int, float)):
            return not (isinstance(val, float) and math.isnan(val))
        if isinstance(val, str):
            s = val.strip().lstrip("([{").rstrip(")]}")
            if not s: return False
            if s.endswith("%"):
                try: 
                    float(s[:-1])
                    return True
                except ValueError: pass
            try:
                float(s.replace(",", ""))
                return True
            except ValueError:
                return False
        return False

    @staticmethod
    def _is_val_empty(v: Any) -> bool:
        return v is None or (isinstance(v, str) and not v.strip())

    @staticmethod
    def _norm_text(v: Any) -> str:
        if v is None: return ""
        return str(v).strip()

    @staticmethod
    def _ensure_unique(names: List[str]) -> List[str]:
        seen = {}
        out = []
        for n in names:
            base = n if n else "unnamed"
            if base not in seen:
                seen[base] = 1
                out.append(base)
            else:
                seen[base] += 1
                out.append(f"{base}_{seen[base]}")
        return out

if __name__ == "__main__":
    # Example usage
    # parser = ExcelParser("/Users/dhirenoswal/Desktop/TU corpus/NonStandardParser/Correspondence/notebook/frank1999.xls")
    # parser = ExcelParser("/Users/dhirenoswal/Desktop/TU corpus/ExcelParser/Data/orig-ocean99-xls/Clemens/Clemens1996/clemens1996.xls")
    # parser = ExcelParser("/Users/dhirenoswal/Desktop/TU corpus/ExcelParser/Data/orig-ocean99-xls/Ishiwatari/ishiwatari1999.xls")
    parser = ExcelParser("/Users/dhirenoswal/Desktop/TU corpus/ExcelParser/Data/orig-ocean99-xls/Overpeck1996/overpeck1996.xls")
    # parser = ExcelParser("/Users/dhirenoswal/Desktop/TU corpus/ExcelParser/Data/orig-ocean99-xls/Bond/bond1992.xls")
    # parser = ExcelParser("/Users/dhirenoswal/Desktop/TU corpus/ExcelParser/Data/orig-ocean99-xls/Charles/charles1996.xls")
    
    blocks = parser.parse()

    for block in blocks:
        print(f"Block ID: {block.idx}, Type: {block.block_type}, Sheet: {block.sheet_name}")
        if block.df is not None:
            print(block.df.head())
            print(block.df.shape)