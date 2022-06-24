'''Python implementation of ReworkCSS's CSS parser.

Author: Rick Viscomi <rick@httparchive.org>
'''

import re


COMMENT_PATTERN = re.compile(r'\/\*[^*]*\*+([^/*][^*]*\*+)*\/')


class CSSParser():
    '''CSS parser.'''

    def __init__(self, css, options=None):
        self.css = css
        if options is None:
            options = {}
        self.options = options

        # Positional variables.
        self.line = 1
        self.column = 1
        self.errors_list = []


    def parse(self):
        '''Entrypoint for CSS parsing.'''
        return self.stylesheet()


    def update_position(self, val):
        '''Update line and column based on `val`.'''
        lines = re.findall(r'\n', val)
        self.line = self.line + len(lines)
        i = val.rfind('\n')
        if i == -1:
            self.column = self.column + len(val)
        else:
            self.column = len(val) - i


    def position(self):
        '''Mark position.'''
        def _(node):
            self.whitespace()
            return node

        return _


    def whitespace(self):
        '''Parse whitespace.'''
        self.match(r'^\s*')


    def comments(self, rules):
        '''Parse comments.'''
        rules = rules or []
        while self.comment() is not None:
            pass
        return rules


    def comment(self):
        '''Parse comment.'''
        pos = self.position()
        if not self.css or self.css[0] != '/' or self.css[1] != '*':
            return None

        i = 2
        while self.css[i] != '' and (self.css[i] != '*' or self.css[i + 1] != '/'):
            i = i + 1

        i = i + 2

        if self.css[i - 1] == '':
            return self.error('End of self.comment missing')

        val = self.css[2:i-2]
        self.column = self.column + 2
        self.update_position(val)
        self.css = self.css[i:]
        self.column = self.column + 2

        return pos({
            'type': 'self.comment',
            self.comment: val
        })


    def match(self, pattern):
        '''Match `pattern` and return captures.'''
        m = re.match(pattern, self.css)
        if not m:
            return None

        val = m[0]
        self.update_position(val)
        self.css = self.css[len(val):]
        return m


    def error(self, msg):
        '''Error `msg`.'''
        err = f'{self.line}:{self.column}:{msg}'

        if self.options.get('silent'):
            self.errors_list.append(err)
        else:
            raise Exception(err)


    def stylesheet(self):
        '''Parse stylesehet.'''
        rules_list = self.rules()

        return {
            'type': 'stylesheet',
            'stylesheet': {
                'source': self.options.get('source'),
                'rules': rules_list,
                'parsingErrors': self.errors_list
            }
        }


    def check_open(self):
        '''Opening brace.'''
        return self.match(r'^{\s*')


    def check_close(self):
        '''Closing brace.'''
        return self.match(r'^}')


    def rules(self):
        '''Parse ruleset.'''
        rules = []
        self.whitespace()
        self.comments(rules)

        while len(self.css) and self.css[0] != '}' and (node := self.atrule() or self.rule()):
            if node is not False:
                rules.append(node)
                self.comments(rules)

        return rules


    def rule(self):
        '''Parse rule.'''
        pos = self.position()
        sel = self.selector()

        if not sel:
            return self.error('selector missing')

        self.comments([])

        return pos({
            'type': 'rule',
            'selectors': sel,
            'declarations': self.declarations()
        })


    def selector(self):
        '''Parse selector.'''
        m = self.match(r'^([^{]+)')
        if not m:
            return None

        # @fix Remove all self.comments from selectors
        # http://ostermiller.org/findself.comment.html
        s = m[0]
        s = CSSParser.trim(s)
        s = re.sub(r'\/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*\/+', '', s)
        s = re.sub(r'"(?:\\"|[^"])*"|\'(?:\\\'|[^\'])*', lambda m: re.sub(r',', '\u200C', m[0]), s)
        s_list = re.split(r'\s*(?![^(]*\)),\s*', s)
        return [re.sub('\u200C', ',', s) for s in s_list]


    def declarations(self):
        '''Parse declarations'''
        decls = []

        if not self.check_open():
            return self.error('missing "{"')

        self.comments(decls)

        # declarations
        decl = None
        while decl := self.declaration():
            if decl is False:
                continue
            decls.append(decl)
            self.comments(decls)

        if not self.check_close():
            return self.error('missing "}"')

        return decls


    def declaration(self):
        '''Parse declaration.'''
        pos = self.position()

        # prop
        prop = self.match(r'^(\*?[-#\/\*\\\w]+(\[[0-9a-z_-]+\])?)\s*')
        if not prop:
            return None
        prop = CSSParser.trim(prop[0])

        # :
        if not self.match(r'^:\s*'):
            return self.error('property missing ":"\n\n' + prop)

        # val
        val = self.match(r'^((?:\'(?:\\\'|.)*?\'|"(?:\\"|.)*?"|\([^\)]*?\)|[^};])+)')

        ret = pos({
            'type': 'declaration',
            'property': re.sub(COMMENT_PATTERN, '', prop),
            'value': re.sub(COMMENT_PATTERN, '', CSSParser.trim(val[0])) if val else ''
        })

        # ;
        self.match(r'^[;\s]*')

        return ret


    def keyframe(self):
        '''Parse keyframe.'''
        vals = []
        pos = self.position()

        while m := self.match(r'^((\d+\.\d+|\.\d+|\d+)%?|[a-z]+)\s*'):
            vals.append(m[1])
            self.match(r'^,\s*')

        if len(vals) == 0:
            return None

        return pos({
            'type': 'keyframe',
            'values': vals,
            'declarations': self.declarations()
        })


    def atkeyframes(self):
        '''Parse keyframes.'''
        pos = self.position()
        m = self.match(r'^@([-\w]+)?keyframes\s*')

        if not m:
            return None

        vendor = m[1]

        # identifier
        m = self.match(r'^([-\w]+)\s*')
        if not m:
            return self.error('@keyframes missing name')
        name = m[1]

        if not self.check_open():
            return self.error('@keyframes missing "{"')

        frames = self.comments([])
        while frame := self.keyframe():
            frames.append(frame)
            frames = frames + self.comments([])

        if not self.check_close():
            return self.error('@keyframes missing "}"')

        return pos({
            'type': 'keyframes',
            'name': name,
            'vendor': vendor,
            'keyframes': frames
        })


    def atsupports(self):
        '''Parse supports.'''
        pos = self.position()
        m = self.match(r'@supports *([^{]+)')

        if not m:
            return None

        supports = CSSParser.trim(m[1])

        if not self.check_open():
            return self.error('@supports missing "{"')

        style = self.comments([]) + self.rules()

        if not self.check_close():
            return self.error('@supports missing "}"')

        return pos({
            'type': 'supports',
            'supports': supports,
            'rules': style
        })


    def athost(self):
        '''Parse host.'''
        pos = self.position()
        m = self.match(r'^@host\s*')

        if not m:
            return None

        if not self.check_open():
            return self.error('@host missing "{"')

        style = self.comments([]) + self.rules()

        if not self.check_close():
            return self.error('@host missing "}"')

        return pos({
            'type': 'host',
            'rules': style
        })


    def atmedia(self):
        '''Parse media.'''
        pos = self.position()
        m = self.match(r'^@media *([^{]+)')

        if not m:
            return None

        media = CSSParser.trim(m[1])

        if not self.check_open():
            return self.error('@media missing "{"')

        style = self.comments([]) + self.rules()

        if not self.check_close():
            return self.error('@media missing "}')

        return pos({
            'type': 'media',
            'media': media,
            'rules': style
        })


    def atcustommedia(self):
        '''Parse custom-media.'''
        pos = self.position()
        m = self.match(r'^@custom-media\s+(--[^\s]+)\s*([^{;]+);')

        if not m:
            return None

        return pos({
            'type': 'custom-media',
            'name': CSSParser.trim(m[1]),
            'media': CSSParser.trim(m[2])
        })


    def atpage(self):
        '''Parse page media.'''
        pos = self.position()
        m = self.match(r'^@page *')

        if not m:
            return None

        sel = self.selector() or []

        if not self.check_open():
            return self.error('@page missing "{"')

        decls = self.comments([])

        # declarations
        while decl := self.declaration():
            decls.append(decl)
            decls = decls + self.comments([])

        if not self.check_close():
            return self.error('@page missing "}"')

        return pos({
            'type': 'page',
            'selectors': sel,
            'declarations': decls
        })


    def atdocument(self):
        '''Parse document.'''
        pos = self.position()
        m = self.match(r'^@([-\w]+)?document *([^{]+)')

        if not m:
            return None

        vendor = CSSParser.trim(m[1])
        doc = CSSParser.trim(m[2])

        if not self.check_open():
            return self.error('@document missing "{"')

        style = self.comments([]) + self.rules()

        if not self.check_close():
            return self.error('@document missing "}"')

        return pos({
            'type': 'document',
            'document': doc,
            'vendor': vendor,
            'rules': style
        })


    def atfontface(self):
        '''Parse font-face.'''
        pos = self.position()
        m = self.match(r'^@font-face\s*')

        if not m:
            return None

        if not self.check_open():
            return self.error('@font-face missing "{"')

        decls = self.comments([])

        # declarations
        while decl := self.declaration():
            decls.append(decl)
            decls = decls + self.comments([])

        if not self.check_close():
            return self.error('@font-face missing "}"')

        return pos({
            'type': 'font-face',
            'declarations': decls
        })


    def atproperty(self):
        '''Parse property.'''
        pos = self.position()
        m = self.match(r'^@property\s*')

        if not m:
            return None

        m = self.match(r'\s*(--[-\w]+)\s*')
        if not m:
            return self.error('@property --property name missing')
        prop = m[1]

        if not self.check_open():
            return self.error('@property missing "{"')

        decls = self.comments([])

        # declarations
        while decl := self.declaration():
            decls.append(decl)
            decls = decls + self.comments([])

        if not self.check_close():
            return self.error('@property missing "}"')

        return pos({
            'type': 'property',
            'property': prop,
            'declarations': decls
        })


    def parse_at_rule(self, name):
        '''Parse non-block at-rules.'''
        pattern = re.compile(r'^@' + name + r'\s*([^;]+);')

        pos = self.position()
        m = self.match(pattern)
        if not m:
            return None
        ret = {
            'type': name
        }
        ret[name] = CSSParser.trim(m[1])
        return pos(ret)


    def atimport(self):
        '''Parse import.'''
        return self.parse_at_rule('import')


    def atcharset(self):
        '''Parse charset.'''
        return self.parse_at_rule('charset')


    def atnamespace(self):
        '''Parse namespace.'''
        return self.parse_at_rule('namespace')


    def atrule(self):
        '''Parse at rule.'''
        if self.css[0] != '@':
            return None

        return self.atkeyframes() or \
            self.atmedia() or \
            self.atcustommedia() or \
            self.atsupports() or \
            self.atimport() or \
            self.atcharset() or \
            self.atnamespace() or \
            self.atdocument() or \
            self.atpage() or \
            self.athost() or \
            self.atfontface() or \
            self.atproperty()



    @staticmethod
    def trim(val):
        '''Collapse all whitespace, then trim.'''
        if not val:
            return ''

        val = re.sub(r'\s+', ' ', val)
        val = re.sub(r'^\s+|\s+$', '', val)
        return val
