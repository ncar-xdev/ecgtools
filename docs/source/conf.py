# -*- coding: utf-8 -*-


import datetime
import os
import sys

import ecgtools

cwd = os.getcwd()
parent = os.path.dirname(cwd)
sys.path.insert(0, parent)


# -- General configuration -----------------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.
# needs_sphinx = '1.0'

# Add any Sphinx extension module names here, as strings. They can be extensions
# coming with Sphinx (named 'sphinx.ext.*') or your custom ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    'sphinx.ext.autosummary',
    'sphinx.ext.doctest',
    'sphinx.ext.intersphinx',
    'sphinx.ext.extlinks',
    'sphinx.ext.intersphinx',
    # 'IPython.sphinxext.ipython_console_highlighting',
    # 'IPython.sphinxext.ipython_directive',
    'sphinx.ext.napoleon',
    'myst_nb',
    'sphinxext.opengraph',
    'sphinx_copybutton',
    'sphinx_comments',
]

extlinks = {
    'issue': ('https://github.com/NCAR/ecgtools/issues/%s', 'GH#'),
    'pr': ('https://github.com/NCAR/ecgtools/pull/%s', 'GH#'),
}


autodoc_member_order = 'groupwise'

# MyST config
myst_enable_extensions = ['amsmath', 'colon_fence', 'deflist', 'html_image']
myst_url_schemes = ('http', 'https', 'mailto')

comments_config = {
    'utterances': {'repo': 'NCAR/ecgtools', 'optional': 'config', 'label': '💬 comment'},
    'hypothesis': False,
}

jupyter_execute_notebooks = 'off'

# sphinx-copybutton configurations
copybutton_prompt_text = r'>>> |\.\.\. |\$ |In \[\d*\]: | {2,5}\.\.\.: | {5,8}: '
copybutton_prompt_is_regexp = True

# Autosummary pages will be generated by sphinx-autogen instead of sphinx-build
autosummary_generate = []
autodoc_typehints = 'none'

# Napoleon configurations

napoleon_google_docstring = False
napoleon_numpy_docstring = True
napoleon_use_param = False
napoleon_use_rtype = False
napoleon_preprocess_types = True


# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']


# The master toctree document.
master_doc = 'index'

# General information about the project.
project = 'ecgtools'
current_year = datetime.datetime.now().year
copyright = f'2020-{current_year}, NCAR XDev Team'
author = 'NCAR XDev Team'
# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The short X.Y version.
version = ecgtools.__version__.split('+')[0]
# The full version, including alpha/beta/rc tags.
release = ecgtools.__version__

exclude_patterns = ['_build', '**.ipynb_checkpoints', 'Thumbs.db', '.DS_Store']


# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'


# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = 'sphinx_book_theme'
html_title = ''

html_context = {
    'github_user': 'NCAR',
    'github_repo': 'ecgtools',
    'github_version': 'main',
    'doc_path': 'docs',
}
html_theme_options = dict(
    # analytics_id=''  this is configured in rtfd.io
    # canonical_url="",
    repository_url='https://github.com/NCAR/ecgtools',
    repository_branch='main',
    path_to_docs='docs',
    use_edit_page_button=True,
    use_repository_button=True,
    use_issues_button=True,
    home_page_in_toc=False,
    github_url='https://github.com/NCAR/ecgtools',
    twitter_url='https://twitter.com/NCARXDev',
    extra_navbar='',
    navbar_footer_text='',
    extra_footer="""Theme by the <a href="https://ebp.jupyterbook.org">Executable Book Project</a>""",
)

# Output file base name for HTML help builder.
htmlhelp_basename = 'ecgtoolsdoc'


# -- Options for LaTeX output --------------------------------------------------

latex_elements = {
    # The paper size ('letterpaper' or 'a4paper').
    # 'papersize': 'letterpaper',
    # The font size ('10pt', '11pt' or '12pt').
    # 'pointsize': '10pt',
    # Additional stuff for the LaTeX preamble.
    # 'preamble': '',
}

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title, author, documentclass [howto/manual]).
latex_documents = [
    ('index', 'ecgtools.tex', 'ecgtools Documentation', 'NCAR XDev Team', 'manual'),
]

# -- Options for manual page output --------------------------------------------

# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [('index', 'ecgtools', 'ecgtools Documentation', [author], 1)]

# If true, show URL addresses after external links.
# man_show_urls = False


# -- Options for Texinfo output ------------------------------------------------

# Grouping the document tree into Texinfo files. List of tuples
# (source start file, target name, title, author,
#  dir menu entry, description, category)
texinfo_documents = [
    (
        'index',
        'ecgtools',
        'ecgtools Documentation',
        author,
        'ecgtools',
        'One line description of project.',
        'Miscellaneous',
    ),
]

# Documents to append as an appendix to all manuals.
# texinfo_appendices = []

# If false, no module index is generated.
# texinfo_domain_indices = True

# How to display URL addresses: 'footnote', 'no', or 'inline'.
# texinfo_show_urls = 'footnote'

# If true, do not generate a @detailmenu in the "Top" node's menu.
# texinfo_no_detailmenu = False

intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'xarray': ('http://xarray.pydata.org/en/stable/', None),
}
