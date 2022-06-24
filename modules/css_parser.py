'''Python implementation of ReworkCSS's CSS parser.

Author: Rick Viscomi <rick@httparchive.org>
'''

import re


COMMENT_PATTERN = re.compile(r'\/\*[^*]*\*+([^/*][^*]*\*+)*\/')

# Positional variables.
LINE = 1
COLUMN = 1

CSS = None


def trim(val):
    '''Collapse all whitespace, then trim.'''
    if not val:
        return ''

    val = re.sub(r'\s+', ' ', val)
    val = re.sub(r'^\s+|\s+$', '', val)
    return val


def parse(css, options=None):
    '''Entrypoint for CSS parsing.'''
    global CSS
    CSS = css

    if options is None:
        options = {}

    def update_position(val):
        '''Update LINE and COLUMN based on `str`.'''
        global LINE
        global COLUMN

        lines = re.findall(r'\n', val)
        LINE = LINE + len(lines)
        i = val.rfind('\n')
        if i == -1:
            COLUMN = COLUMN = len(val)
        else:
            COLUMN = len(val) - i


    def position():
        '''Mark position.'''
        def _(node):
            whitespace()
            return node

        return _


    def whitespace():
        '''Parse whitespace.'''
        match(r'^\s*')


    def comments(rules):
        '''Parse comments.'''
        rules = rules or []
        while comment() is not None:
            pass
        return rules


    def comment():
        '''Parse comment.'''
        global COLUMN, CSS

        pos = position()
        if not CSS or CSS[0] != '/' or CSS[1] != '*':
            return None

        i = 2
        while CSS[i] != '' and (CSS[i] != '*' or CSS[i + 1] != '/'):
            i = i + 1

        i = i + 2

        if CSS[i - 1] == '':
            return error('End of comment missing')

        val = CSS[2:i-2]
        COLUMN = COLUMN + 2
        update_position(val)
        CSS = CSS[i:]
        COLUMN = COLUMN + 2

        return pos({
            'type': 'comment',
            comment: val
        })


    def match(pattern):
        '''Match `pattern` and return captures.'''
        global CSS

        m = re.match(pattern, CSS)
        if not m:
            return None

        val = m[0]
        update_position(val)
        CSS = CSS[len(val):]
        return m


    errors_list = []
    def error(msg):
        '''Error `msg`.'''
        global LINE
        global COLUMN

        err = f'{LINE}:{COLUMN}:{msg}'

        if options.get('silent'):
            errors_list.append(err)
        else:
            raise Exception(err)


    def stylesheet():
        '''Parse stylesehet.'''
        rules_list = rules()

        return {
            'type': 'stylesheet',
            'stylesheet': {
                'source': options.get('source'),
                'rules': rules_list,
                'parsingErrors': errors_list
            }
        }


    def check_open():
        '''Opening brace.'''
        return match(r'^{\s*')


    def check_close():
        '''Closing brace.'''
        return match(r'^}')


    def rules():
        '''Parse ruleset.'''
        global CSS

        rules = []
        whitespace()
        comments(rules)

        while len(CSS) and CSS[0] != '}' and (node := atrule() or rule()):
            if node is not False:
                rules.append(node)
                comments(rules)

        return rules


    def rule():
        '''Parse rule.'''
        pos = position()
        sel = selector()

        if not sel:
            return error('selector missing')

        comments([])

        return pos({
            'type': 'rule',
            'selectors': sel,
            'declarations': declarations()
        })


    def selector():
        '''Parse selector.'''
        m = match(r'^([^{]+)')
        if not m:
            return None

        # @fix Remove all comments from selectors
        # http://ostermiller.org/findcomment.html
        s = m[0]
        s = trim(s)
        s = re.sub(r'\/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*\/+', '', s)
        s = re.sub(r'"(?:\\"|[^"])*"|\'(?:\\\'|[^\'])*', lambda m: re.sub(r',', '\u200C', m[0]), s)
        s_list = re.split(r'\s*(?![^(]*\)),\s*', s)
        return [re.sub('\u200C', ',', s) for s in s_list]


    def declarations():
        '''Parse declarations'''
        decls = []

        if not check_open():
            return error('missing "{"')

        comments(decls)

        # declarations
        decl = None
        while decl := declaration():
            if decl is False:
                continue
            decls.append(decl)
            comments(decls)

        if not check_close():
            return error('missing "}"')

        return decls


    def declaration():
        '''Parse declaration.'''
        global COMMENT_PATTERN

        pos = position()

        # prop
        prop = match(r'^(\*?[-#\/\*\\\w]+(\[[0-9a-z_-]+\])?)\s*')
        if not prop:
            return None
        prop = trim(prop[0])

        # :
        if not match(r'^:\s*'):
            return error('property missing ":"')

        # val
        val = match(r'^((?:\'(?:\\\'|.)*?\'|"(?:\\"|.)*?"|\([^\)]*?\)|[^};])+)')

        ret = pos({
            'type': 'declaration',
            'property': re.sub(COMMENT_PATTERN, '', prop),
            'value': re.sub(COMMENT_PATTERN, '', trim(val[0])) if val else ''
        })

        # ;
        match(r'^[;\s]*')

        return ret


    def keyframe():
        '''Parse keyframe.'''
        vals = []
        pos = position()

        while m := match(r'^((\d+\.\d+|\.\d+|\d+)%?|[a-z]+)\s*'):
            vals.append(m[1])
            match(r'^,\s*')

        if len(vals) == 0:
            return None

        return pos({
            'type': 'keyframe',
            'values': vals,
            'declarations': declarations()
        })


    def atkeyframes():
        '''Parse keyframes.'''
        pos = position()
        m = match(r'^@([-\w]+)?keyframes\s*')

        if not m:
            return None

        vendor = m[1]

        # identifier
        m = match(r'^([-\w]+)\s*')
        if not m:
            return error('@keyframes missing name')
        name = m[1]

        if not check_open():
            return error('@keyframes missing "{"')

        frames = comments([])
        while frame := keyframe():
            frames.append(frame)
            frames = frames + comments([])

        if not check_close():
            return error('@keyframes missing "}"')

        return pos({
            'type': 'keyframes',
            'name': name,
            'vendor': vendor,
            'keyframes': frames
        })


    def atsupports():
        '''Parse supports.'''
        pos = position()
        m = match(r'@supports *([^{]+)')

        if not m:
            return None

        supports = trim(m[1])

        if not check_open():
            return error('@supports missing "{"')

        style = comments([]) + rules()

        if not check_close():
            return error('@supports missing "}"')

        return pos({
            'type': 'supports',
            'supports': supports,
            'rules': style
        })


    def athost():
        '''Parse host.'''
        pos = position()
        m = match(r'^@host\s*')

        if not m:
            return None

        if not check_open():
            return error('@host missing "{"')

        style = comments([]) + rules()

        if not check_close():
            return error('@host missing "}"')

        return pos({
            'type': 'host',
            'rules': style
        })


    def atmedia():
        '''Parse media.'''
        pos = position()
        m = match(r'^@media *([^{]+)')

        if not m:
            return None

        media = trim(m[1])

        if not check_open():
            return error('@media missing "{"')

        style = comments([]) + rules()

        if not check_close():
            return error('@media missing "}')

        return pos({
            'type': 'media',
            'media': media,
            'rules': style
        })


    def atcustommedia():
        '''Parse custom-media.'''
        pos = position()
        m = match(r'^@custom-media\s+(--[^\s]+)\s*([^{;]+);')

        if not m:
            return None

        return pos({
            'type': 'custom-media',
            'name': trim(m[1]),
            'media': trim(m[2])
        })


    def atpage():
        '''Parse page media.'''
        pos = position()
        m = match(r'^@page *')

        if not m:
            return None

        sel = selector() or []

        if not check_open():
            return error('@page missing "{"')

        decls = comments([])

        # declarations
        while decl := declaration():
            decls.append(decl)
            decls = decls + comments([])

        if not check_close():
            return error('@page missing "}"')

        return pos({
            'type': 'page',
            'selectors': sel,
            'declarations': decls
        })


    def atdocument():
        '''Parse document.'''
        pos = position()
        m = match(r'^@([-\w]+)?document *([^{]+)')

        if not m:
            return None

        vendor = trim(m[1])
        doc = trim(m[2])

        if not check_open():
            return error('@document missing "{"')

        style = comments([]) + rules()

        if not check_close():
            return error('@document missing "}"')

        return pos({
            'type': 'document',
            'document': doc,
            'vendor': vendor,
            'rules': style
        })


    def atfontface():
        '''Parse font-face.'''
        pos = position()
        m = match(r'^@font-face\s*')

        if not m:
            return None

        if not check_open():
            return error('@font-face missing "{"')

        decls = comments([])

        # declarations
        while decl := declaration():
            decls.append(decl)
            decls = decls + comments([])

        if not check_close():
            return error('@font-face missing "}"')

        return pos({
            'type': 'font-face',
            'declarations': decls
        })


    def atproperty():
        '''Parse property.'''
        pos = position()
        m = match(r'^@property\s*')

        if not m:
            return None

        m = match(r'\s*(--[-\w]+)\s*')
        if not m:
            return error('@property --property name missing')
        prop = m[1];

        if not check_open():
            return error('@property missing "{"')

        decls = comments([])

        # declarations
        while decl := declaration():
            decls.append(decl)
            decls = decls + comments([])

        if not check_close():
            return error('@property missing "}"')

        return pos({
            'type': 'property',
            'property': prop,
            'declarations': decls
        })


    def _compile_at_rule(name):
        '''Parse non-block at-rules.'''
        pattern = re.compile(r'^@' + name + r'\s*([^;]+);')

        def _():
            pos = position()
            m = match(pattern)
            if not m:
                return None
            ret = {
                'type': name
            }
            ret[name] = trim(m[1])
            return pos(ret)
        return _


    # Parse import
    atimport = _compile_at_rule('import')

    # Parse charset
    atcharset = _compile_at_rule('charset')

    # Parse namespace
    atnamespace = _compile_at_rule('namespace')


    def atrule():
        '''Parse at rule.'''
        global CSS

        if CSS[0] != '@':
            return None

        return atkeyframes() or \
            atmedia() or \
            atcustommedia() or \
            atsupports() or \
            atimport() or \
            atcharset() or \
            atnamespace() or \
            atdocument() or \
            atpage() or \
            athost() or \
            atfontface() or \
            atproperty()


    return stylesheet()
