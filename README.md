# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/HTTPArchive/data-pipeline/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                              |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|---------------------------------- | -------: | -------: | -------: | -------: | ------: | --------: |
| modules/combined\_pipeline.py     |       47 |        0 |        4 |        0 |    100% |           |
| modules/constants.py              |       12 |        0 |        0 |        0 |    100% |           |
| modules/import\_all.py            |      256 |      211 |       80 |        0 |     13% |28-100, 106-143, 149-189, 199-238, 244-268, 274-382, 388, 394-400, 406-412, 441-444, 454-458, 462-474 |
| modules/non\_summary\_pipeline.py |      243 |      175 |       80 |        2 |     24% |23-52, 66-72, 76-78, 82-87, 115-170, 177-179, 185-191, 202-237, 243-287, 293-333, 347-399, 428-431, 437-441, 447-462, 512->exit, 522->exit |
| modules/summary\_pipeline.py      |       19 |        0 |        6 |        0 |    100% |           |
| modules/transformation.py         |      258 |      141 |       84 |        1 |     41% |31->exit, 34-36, 202-221, 225-418, 504-670 |
| modules/utils.py                  |      135 |        1 |       75 |        1 |     99% |       222 |
|                         **TOTAL** |  **970** |  **528** |  **329** |    **4** | **43%** |           |

2 empty files skipped.


## Setup coverage badge

Below are examples of the badges you can use in your main branch `README` file.

### Direct image

[![Coverage badge](https://raw.githubusercontent.com/HTTPArchive/data-pipeline/python-coverage-comment-action-data/badge.svg)](https://htmlpreview.github.io/?https://github.com/HTTPArchive/data-pipeline/blob/python-coverage-comment-action-data/htmlcov/index.html)

This is the one to use if your repository is private or if you don't want to customize anything.

### [Shields.io](https://shields.io) Json Endpoint

[![Coverage badge](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/HTTPArchive/data-pipeline/python-coverage-comment-action-data/endpoint.json)](https://htmlpreview.github.io/?https://github.com/HTTPArchive/data-pipeline/blob/python-coverage-comment-action-data/htmlcov/index.html)

Using this one will allow you to [customize](https://shields.io/endpoint) the look of your badge.
It won't work with private repositories. It won't be refreshed more than once per five minutes.

### [Shields.io](https://shields.io) Dynamic Badge

[![Coverage badge](https://img.shields.io/badge/dynamic/json?color=brightgreen&label=coverage&query=%24.message&url=https%3A%2F%2Fraw.githubusercontent.com%2FHTTPArchive%2Fdata-pipeline%2Fpython-coverage-comment-action-data%2Fendpoint.json)](https://htmlpreview.github.io/?https://github.com/HTTPArchive/data-pipeline/blob/python-coverage-comment-action-data/htmlcov/index.html)

This one will always be the same color. It won't work for private repos. I'm not even sure why we included it.

## What is that?

This branch is part of the
[python-coverage-comment-action](https://github.com/marketplace/actions/python-coverage-comment)
GitHub Action. All the files in this branch are automatically generated and may be
overwritten at any moment.