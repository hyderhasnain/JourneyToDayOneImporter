# Journey to Day One Importer

**NOTE: This fork is based on the original developed by user camflint. My updated code is in file j2d_regex_conversions.py. My updated code allows all formatting in the original Journey file, which is stored as HTML, to be converted correctly to the Markdown formatting used by Day One. In my testing, the original code stripped out this formatting.**

**The following are the contents of camflint's original README. In order to run the code in this fork, use j2d_regex_conversions.py instead of j2d.py**

---


> **NOTICE**: I am not affiliated with the companies behind these applications. I created this script for my own use. Use at your own discretion.

A command-line tool to migrate your Journey entries to Day One.

Features:

* Includes photos
* Includes tags
* Includes geocoordinates
* Includes timestamp (timezone aware)
* Imported text looks as expected (no formatting/encoding issues)

Prerequisites:

1. Clone the repository and execute `pip3 install -r requirements.txt`.
1. Make sure the `dayone2` executable is on your PATH (select DayOne > Install Command Line Tools...).
1. Export your journal from Journey as a .zip.
1. Extract the resulting .zip file into a directory.
   
With these instructions complete, you should have a `source_directory` with all of your exported Journey entries. Given this and a `target_journal_name`, run the following from a Terminal window:

```shell
$ python3 j2d.py <target_journal_name> <source_directory>
```

Example output:

```shell
WARNING: coordinates are invalid: 1.7976931348623157e+308 1.7976931348623157e+308
[1/508] Added new: 1532920577071-bxzfmf02n8r96vb5 -> 151FDBF1CB1B4F658C261131AD7296D4: 66 words, 8 tags, 6 photos
[2/508] Added new: 1590644431033-3b8kodxeyhfwatze -> 5766077480B3478AA5744906752B7E9A: 9 words
[3/508] Added new: 1549735900606-555qqtpidpf8zjne -> 661865B88EC441599645B1FBCB3FD5C8: 12 words, 2 tags
[4/508] Added new: 1525981267299-j6iz9i1ajnkg4ofw -> 5937EA89A1794410975E8C22EEB65EFD: 10 words, 3 tags
[5/508] Added new: 1513562853667-3fc7acbe265c58d0 -> 9D743D63A097476694BA297FA4FFA80D: 12 words, 4 tags, 2 photos
[6/508] Added new: 1590649634456-q8d3kalowuq19lx7 -> A395946A089B4D3DA4D33553E09D73C5: 4 tags, 1 photos
[7/508] Added new: 1515011399977-3fdb529186e4fc32 -> 6717268DB7C24B678A7E8BFB0F2A9615: 29 words, 3 tags
[8/508] Added new: 1529389558714-1r4s6scsr96di5yq -> 913F75A728154FCBA1D8D50DC94B5E98: 25 words, 6 tags, 3 photos
[9/508] Added new: 1538105531793-846iyuspmeh91svn -> BC1F204F2749437A8E6AC82A6869A02D: 2 words, 4 tags, 3 photos
...

508 succeeded, 0 failed, 1 skipped
```
