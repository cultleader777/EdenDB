if exists("b:current_syntax")
  finish
endif

syntax include @SQL syntax/sql.vim
syntax include @LUA syntax/lua.vim
syntax include @PROLOG syntax/prolog.vim

syn keyword dataRegionKeywords WITH

syn keyword basicTypes INT BOOL FLOAT TEXT contained
syn keyword columnKeywords REF PRIMARY KEY CHILD OF UNIQUE CHECK contained

syn region celTableBlock transparent fold matchgroup=outerStatement start="TABLE\s\+[a-z0-9_]\+\s\+{" end="}" contains=basicTypes,columnKeywords
syn region celMViewBlock transparent fold matchgroup=outerStatement start="MATERIALIZED\s\+VIEW\s\+[a-z0-9_]\+\s\+{" end="}\s\+AS\s\+" contains=basicTypes,columnKeywords nextGroup=embeddedSql

syn region embeddedLua fold transparent matchgroup=outerStatement start="INCLUDE\s\+LUA\s\+{" end="}" contains=@LUA

syn region embeddedSql matchgroup=outerStatement start="{" end="}" contains=@SQL contained

syn region proofDatalog fold transparent matchgroup=outerStatement start="PROOF\s\+.*DATALOG\s\+{" end="}" contains=@PROLOG

syn region proofSql fold transparent matchgroup=outerStatement start="PROOF\s\+.*{" end="}" contains=@SQL

syn region celDataBlock transparent fold matchgroup=outerStatement start="DATA\s\+[a-z0-9_]\+.*{" end="}" contains=dataRegionKeywords

syn region  celCommentBlock	start="//" skip="\\$" end="$" keepend

let b:current_syntax = "edl"

hi def link columnKeywords     Statement
hi def link basicTypes         Type
hi def link outerStatement     Function
hi def link dataRegionKeywords Operator
hi def link celCommentBlock    Comment
