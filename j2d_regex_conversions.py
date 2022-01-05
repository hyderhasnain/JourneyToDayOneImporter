import math
import os
import re
import regex
import sys
import json
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Iterable, Optional, Tuple

from bs4 import BeautifulSoup
from pytz import timezone as tz, UnknownTimeZoneError
from tzlocal import get_localzone

@dataclass
class JourneyEntry:
    id: str
    path: str
    date_journal: int
    text: str
    type: str
    lat: float
    lon: float
    timezone: str
    address: str
    tags: List[str]
    photos: List[str]


@dataclass
class ValidatedEntry:
    foreign_id: str
    source_path: str
    text: str
    tags: List[str]
    photos: List[str]
    lat: Optional[float]
    lon: Optional[float]
    timestamp: str
    timezone: str


@dataclass
class ImportOneResult:
    image_count: int = 0
    tag_count: int = 0
    word_count: int = 0


@dataclass
class ImportManyResult:
    failed_paths: List[str]
    skipped_paths: List[str]
    attempted_count: int = 0
    total_count: int = 0


def parse_args():
    args = sys.argv
    if len(args) < 3:
        raise RuntimeError('missing [dest_journal_name] and/or [src_directory] arguments')
    dest_journal_name = sys.argv[1]
    src_directory = os.path.expanduser(sys.argv[2])
    if not os.path.exists(src_directory):
        raise RuntimeError('directory is missing or invalid')
    return dest_journal_name, src_directory


class Importer:
    def __init__(self, src_directory, target_journal_name, debug=False):
        self.src_directory = src_directory
        self.target_journal_name = target_journal_name
        self.debug = debug
        self.data = None

    def run(self):
        self.data = ImportManyResult([], [])
        raw_entries = self.load_journey_entries(self.src_directory)
        valid_entries = self.validate_journey_entries(raw_entries)
        imported_entries = self.import_entries(valid_entries)
        self.print_result(imported_entries)

    def load_journey_entries(self, root) -> Iterable[JourneyEntry]:
        for file in self.iter_journey_files(root):
            yield self.load_entry(file)

    def iter_journey_files(self, root) -> Iterable[str]:
        p = Path(root)
        for file in p.rglob('*.json'):
            yield file

    def load_entry(self, file) -> JourneyEntry:
        with open(file) as f:
            text = f.read()
            body = json.loads(text)
            entry = self.extract_entry_from_body(body, file)
            return entry

    def extract_entry_from_body(self, body, path) -> JourneyEntry:
        return JourneyEntry(id=body['id'], date_journal=body['date_journal'], text=body['text'], lon=body['lon'],
                            lat=body['lat'], tags=body['tags'], photos=body['photos'], address=body['address'],
                            type=body['type'], timezone=body['timezone'], path=path)

    def validate_journey_entries(self, raw_entries: Iterable[JourneyEntry]) -> Iterable[ValidatedEntry]:
        for raw in raw_entries:
            entry = self.build_valid_entry(raw)
            if entry:
                yield entry

    def build_valid_entry(self, raw: JourneyEntry) -> Optional[ValidatedEntry]:
        foreign_id = raw.id
        source_path = raw.path

        timezone = ''
        if raw.timezone:
            try:
                timezone = tz(raw.timezone).zone
            except UnknownTimeZoneError:
                print('WARNING: timezone is invalid: {}'.format(raw.timezone))
        if not timezone:
            timezone = get_localzone().zone

        timestamp = ''
        timestamp_format = '%Y-%m-%d %I:%M:%S %p'
        if raw.date_journal is not None:
            try:
                dt = datetime.fromtimestamp(raw.date_journal / 1000)
                timestamp = dt.strftime(timestamp_format)
            except (OverflowError, ValueError, OSError):
                print('WARNING: timestamp is invalid: {}'.format(raw.date_journal))
        if not timestamp:
            dt = datetime.now(tz(timezone))
            timestamp = dt.strftime(timestamp_format)

        photos = []
        if len(raw.photos) > 0:
            for path in raw.photos:
                abs_path = os.path.abspath(os.path.join(self.src_directory, path))
                if os.path.exists(abs_path) and os.path.isfile(abs_path):
                    photos.append(abs_path)
                else:
                    print('WARNING: photo path is invalid: {}'.format(abs_path))

        tags = []
        if len(raw.tags) > 0:
            escaped_tags = map(self.escape_tag, raw.tags)
            tags.extend(escaped_tags)

        lat = None
        lon = None
        if raw.lat is not None and raw.lon is not None:
            is_lat_valid = math.isfinite(raw.lat) and math.fabs(raw.lat) <= 90
            is_lon_valid = math.isfinite(raw.lon) and math.fabs(raw.lon) <= 180
            if is_lat_valid and is_lon_valid:
                lat = raw.lat
                lon = raw.lon
            else:
                print('WARNING: coordinates are invalid: {} {}'.format(raw.lat, raw.lon))

        skip = False
        text = self.convert_journey_html_to_dayone_markdown(raw.text)
        #text = self.strip_text_from_html_body(raw.text)
        if not text:
            print('WARNING: entry has no text: id={}'.format(raw.id))
        if not text and len(photos) == 0:
            print('WARNING: entry has no text and no photos, skipping: id={}'.format(raw.id))
            skip = True
        if text.find('dayone-moment:') != -1:
            print('WARNING: skipped previously-exported DayOne entry: id={}'.format(raw.id))
            skip = True

        if not skip:
            return ValidatedEntry(foreign_id=foreign_id, source_path=source_path, text=text, photos=photos, tags=tags,
                                  lat=lat, lon=lon, timestamp=timestamp, timezone=timezone)
        else:
            self.data.skipped_paths.append(source_path)
            return None

    def escape_tag(self, raw):
        tag = re.sub(r'\s+', r'\\\g<0>', raw)
        return tag


    def convert_journey_html_to_dayone_markdown(self, text):
        ## Perform text replacements of HTML elements in journey data with markdown elements needed for day one to interpret. Use regex for this.
        # HTML tables will not be replaced due to complexity and laziness

        text = self.convert_simple(text)
        text = self.convert_lists(text)
        text = self.perform_phrase_replacements(text)
        text = self.fix_name_spellings(text)
        text = self.convert_quote_blocks(text)

        # Replace horizontal lines
        text = re.sub(r'<hr( dir.*?){0,1}>', r'\n---', text)

        #text = self.convert_weblinks(text) # This function doesn't work properly and isn't needed currently, might need to implement properly in the future

        return text

    def convert_simple(self, text):
        # Make simple known replacements, 1:1
        ## More details:
        ## In my testing, Journey will sometimes put a &nbsp; (non-breaking space) character before a closing tag of a bold or italic block
        ##  In Journey, that space will be rendered *after* the formatted text
        ##  However, if we simply replace <strong> and <\/strong> with **, and then replace &nbsp; with a space, the closing ** will have an incorrect space before it,
        ##  and will therefore not be applied to the previous text in Day One. So we need to *move* the &nbsp; to *after* the closing tag, and then replace the &nbsp; with a space
        to_replace_simple = (r'<(em|i)>(?P<content>.*?)(?P<nbsp>&nbsp;)*<\\*/+(em|i)>', r'<(strong|b)>(?P<content>.*?)(?P<nbsp>&nbsp;)*<\\*/+(strong|b)>', r'<(del)>(?P<content>.*?)(?P<nbsp>&nbsp;)*<\\*/+(del)>', r'(<span.*?style.*?underline.*?>)(?P<text>.*?\w.*?)(<\\{0,1}/{1}span>)', r'<h1>', r'<h2>', r'<h3>', r'&nbsp;')
        replace_with_simple = (r'*\g<content>*\g<nbsp>', r'**\g<content>**\g<nbsp>', r'~~\g<content>~~\g<nbsp>' , r'*\g<text>*', r'\n# ', r'\n## ', r'\n##### ', r' ')
        to_remove_simple = (r'<p\s.*?>', r'<(\\{0,1}/{1}){0,1}p>', r'<(\\{0,1}/{1}){0,1}h\d+>', r'<(\\{0,1}/{1}){0,1}del>', r'<(\\{0,1}/{1}){0,1}span\s{0,1}.*?>')

        for pair in zip(to_replace_simple, replace_with_simple): # Replace HTML elements with markdown elements
                text=re.sub(pair[0], pair[1], text)

        for to_remove in to_remove_simple: # Remove HTML elements
                text=re.sub(to_remove, r'', text)

        return text

    def convert_lists(self, text):
        ## Convert text for ordered, unordered, and task lists

        ol_parent_search_pattern = r'(<ol>)(\s*)(.*?)(<\\*/+ol>)' #ordered lists
        ul_parent_search_pattern = r'(?P<opening_tag><ul.*?>)(?P<newline>\s*)(?P<contents>.*?)(<\\*/+ul>)' #generic pattern for unordered lists and task lists
        tl_start_search_pattern = r'(<ul\s*class=\"task\">)' #pattern for the opening tag for <ul> task lists, match group 1 of regular ul_parent_search_pattern with this. # using .{1} to represent a single backslash, workaround cause couldn't get it to recognize \\ as a single backslash
        li_search_pattern = r'(<li>)(?P<contents>.*?)(<\\*/+li>)(?P<newline>\s*)' #generic pattern for contents of all lists

        ## Replace ordered lists
        # Process:
        #  Find first instance of pattern <ol> ... <\/ol>
        #  Within first instance, replace each <li> ... <\/li> with corresponding numbered \n#. ...
        #  Replace first instance of <ol> ... <\/ol> with new text
        #  Repeat until all instances of <ol> ... <\/ol> are replaced
        # More details:
        ## We are expecting a text structure like:
        ## <ol> \n <li>Item Text<\/li>\n <li>Item Text<\/li>\n ... <\ol>
        ## The repeating pattern inside in <li>Item Text<\/li>\n
        ## The very first |n after the <ol> is not part of the repeating pattern
        ##  so it has to be treated separately
        ## Parent Pattern: <ol> \n [Repeating Pattern] <\/ol>
        ## Important to make sure that there is no overlap between Parent and Repeating Patterns in what text is found
        ## We have to replace each instance of <ol> ... <\/ol> one-by-one
        ##  in a loop, rather than replacing all intances all at once, because
        ##  1: Each pattern is one non-repeating string plus an arbitrary number of repeating strings
        ##   2: Once we replace any instances, the match indices of the other instances will change so we can no longer
        ##      use those original matches and must re-check all remaining matches. So easier to do one-by-one

        # ORDERED LIST REPLACEMENT:
        ol_match = re.search(ol_parent_search_pattern, text, flags=re.DOTALL) #include \n in group 3
        while ol_match: #Each iteration, we replace one instance of ol_match, so eventually we run out

            ol_substring = text[ol_match.start():ol_match.end()] #substring of full text within the first set of <ol> and <\/ol> tags found
            li_iter = re.finditer(li_search_pattern, ol_substring) #multiple matches, each is

            li_str = '' #This will contain the full replacement for everything spanning from <ol> to <\/ol>, we build it up line by line because each line has its own line number
            for li_index, li_match in enumerate(li_iter, start=1): #build the new text that should replace the entire <ol> ... <\/ol> text
                line_num = r'\n' + str(li_index) + '. '
                li_str = li_str + line_num + li_match.group('contents') + li_match.group('newline')

            #perform actual substitution
            text = re.sub(ol_parent_search_pattern, r'\2' + li_str, text, count=1, flags=re.DOTALL)
            ol_match = re.search(ol_parent_search_pattern, text, flags=re.DOTALL)

        # UNORDERED AND TASK LIST REPLACEMENT:
        ul_match = re.search(ul_parent_search_pattern, text, flags=re.DOTALL)
        while ul_match:

            ul_substring = text[ul_match.start('newline'):ul_match.end('contents')] #only text between <ul> and <\/ul>

            if re.search(tl_start_search_pattern, ul_match.group('opening_tag'), flags=re.DOTALL): #task list
                ul_substring = re.sub(r'<li data-checked=\"true\">', r'\n- [X] ', ul_substring)
                ul_substring = re.sub(r'<li>', r'\n- [ ] ', ul_substring)

            else: # unordered list
                ul_substring = re.sub(r'<li>', r'\n- ', ul_substring)

            ul_substring = re.sub(r'<\\*/+li>', '', ul_substring) # Finally, get rid of closing tags

            text = re.sub(ul_parent_search_pattern, r'\2' + ul_substring, text, count=1, flags=re.DOTALL) # remove <ul> and <\/ul> and put in new \n - ... text. \2 is group for newline char. we only want to replace a single match in this iteration
            ul_match = re.search(ul_parent_search_pattern, text, flags=re.DOTALL)


        return text

    def convert_weblinks(self, text):
        # NOTE: THIS CURRENTLY DOES NOT WORK CORRECTLY
        ## Day One can currently interpret the HTML syntax of <a href...> for web links and hyperlinks correctly and display links correctly, so this function is not needed for now. However, it is probably best practice to convert this HTML syntax into the native format that Day One uses, which is [display text](link itself). Not sure why this function doesn't work though.

        link_parent_search_pattern = r'(<a\s*href=\")(?P<link>.*?)(.{1}\">)(?P<display_text>.*?)(<\\*/+a>)' # using .{1} to represent a single backslash, workaround cause couldn't get it to recognize \\ as a single backslash
        link_parent_replacement_pattern = r'[\g<display_text>\](\g<link>)'

        text = re.sub(link_parent_search_pattern, link_parent_replacement_pattern, text, flags=re.DOTALL)

        return text

    def convert_quote_blocks(self, text):
        # Replace block quotes
        ## Similiar process to how we replaced lists
        ## For any text between <blockquote> and <\/blockquote>, each new line needs to be prepended with \n>
        ## So we must loop through first and find the parent pattern, and then for each matching parent pattern, do substitutions
        ##  only within that part of the text

        quote_parent_search_pattern = r'(<blockquote>)(?P<contents>.*?)(<\\*/+blockquote>)'
        quote_contents_search_pattern = r'(\n)(?P<contents>.*?(?=\n))' # Pattern of \nText followed by a \n, but don't keep the very final \n
        quote_match = re.search(quote_parent_search_pattern, text, flags=re.DOTALL) #DOTALL flag needed to capture newlines
        while quote_match:
            quote_substring = text[quote_match.start('contents'):quote_match.end('contents')]
            quote_substring = re.sub(quote_contents_search_pattern, r'\n' + '> ' + r'\g<contents>', quote_substring, flags=re.DOTALL)

            text = re.sub(quote_parent_search_pattern, quote_substring, text, count=1, flags=re.DOTALL) # Replace one instance of <blockquote> ... <\/blockquote>. We only want to replace a single match in this iteration
            quote_match = re.search(quote_parent_search_pattern, text, flags=re.DOTALL)

        return text

    def perform_phrase_replacements(self, text):
        # Replace certain phrases/words with different phrases/words

        # Replacements performed:
        ## Replace "coffeeshop" with "coffee shop"
        coffee_shop_search_pattern = r'(C|c)(offee)(S|s)(hop)'
        coffee_shop_replacement_pattern = r'\1\2 \3\4'

        text = re.sub(coffee_shop_search_pattern, coffee_shop_replacement_pattern, text)

        return text

    def fix_name_spellings(self, text):
        # Fix spelling and capitalization of names. If a name is already spelled right and is in all caps, then it is not affected

        names_to_fix = {
            'Name One': ['Nome One'],
            'Name Two': ['Nome Two', 'Neme Two'],
            'Name Three': [None]
        } # Format: Key = A correct case name spelling to use, for fixing both mispelled and uncapitalized names. Value = List (!!!) of possible uppercased mispellings of name to fix. If no mispellings for a given name, use value of None

        for correct_name, mispellings in names_to_fix.items():

            # Fix simple capitalization fix going from full lowercase to Uppercase
            text = re.sub(correct_name.lower(), correct_name, text)

            # Fix spelling and also apply correct capitalization (in case original name is all caps)
            for mispelling in mispellings:
                if mispelling is not None:
                    # NOME -> NAME
                    text = re.sub(mispelling.upper(), correct_name.upper(), text)
                    # nome, Nome -> Name
                    text = re.sub(mispelling + '|' + mispelling.lower(), correct_name, text)

        return text

    def strip_text_from_html_body(self, original_text):

        soup = BeautifulSoup(original_text, 'html5lib')
        is_html = bool(soup.find())  # true if at least one HTML element can be found
        if is_html:
            return soup.get_text(strip=True)
        else:
            return original_text
    
    def import_entries(self, entries: Iterable[ValidatedEntry]):
        flat_entries = list(entries)
        self.data.total_count = len(flat_entries)
        for entry in flat_entries:
            id, err = self.import_one_entry(entry)
            self.data.attempted_count += 1
            if not err:
                prefix = '[{}/{}]'.format(self.data.attempted_count, self.data.total_count)
                data = []
                if entry.text:
                    data.append('{} words'.format(len(entry.text.split())))
                if len(entry.tags):
                    data.append('{} tags'.format(len(entry.tags)))
                if len(entry.photos):
                    data.append('{} photos'.format(len(entry.photos)))
                if len(data):
                    print('{} Added new: {} -> {}: {}'.format(prefix, entry.foreign_id, id, ', '.join(data)))
                else:
                    print('{} Added new: {} -> {}'.format(prefix, entry.foreign_id, id))
                yield entry
            else:
                self.data.failed_paths.append(entry.source_path)
                print('ERROR: {}'.format(err))

    def import_one_entry(self, entry: ValidatedEntry) -> Tuple[Optional[str], Optional[str]]: # id, err
        args = self.build_dayone_args(entry)
        if self.debug:
            print(args)
        p = subprocess.run(args, input=entry.text, text=True, capture_output=True)
        if p.returncode == 0:
            id = self.parse_id_from_output(p.stdout)
            return id, None
        else:
            err = p.stderr
            return None, err

    def build_dayone_args(self, entry: ValidatedEntry):
        args = ['dayone2', '-j', self.target_journal_name]
        args.extend(['-d', entry.timestamp])
        args.extend(['-z', entry.timezone])
        if len(entry.tags) > 0:
            args.extend(['-t', *entry.tags])
        if len(entry.photos) > 0:
            args.extend(['-p', *entry.photos])
        if entry.lat and entry.lon:
            args.extend(['--coordinate', str(entry.lat), str(entry.lon)])
        args.extend(['--', 'new'])
        return args

    def parse_id_from_output(self, output):
        # E.g.: "Created new entry with uuid: CB17A357BED34F6D838410CA96C7D9D1"
        m = re.search(r'([A-F0-9]+)\s*$', output)
        if m:
            id = m.group(1)
            return id
        else:
            return ''

    def print_result(self, imported_entries: Iterable[ValidatedEntry]):
        succeeded_count = len(list(imported_entries))
        self.print_paths("SKIPPED", self.data.skipped_paths)
        self.print_paths("FAILED", self.data.failed_paths)
        skipped_count = len(self.data.skipped_paths)
        failed_count = len(self.data.failed_paths)
        print()
        print('{} succeeded, {} failed, {} skipped'.format(succeeded_count, failed_count, skipped_count))

    def print_paths(self, prefix, paths):
        if len(paths):
            print()
        for path in paths:
            print('{}: {}'.format(prefix, path))


if __name__ == '__main__':
    target_journal_name, src_directory = parse_args()
    i = Importer(src_directory, target_journal_name, debug=True)
    i.run()
