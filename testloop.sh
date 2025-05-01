#!/bin/zsh

while true; do
    clear
    # pytest -x -vv $subdir
    # pytest -vv -x tests # ${subdir}
    if [ "$1" = "mypy" ]; then
      make mypy
    else

      #pipenv run pytest .
      make test
    fi

    fswatch -1 **/*.py 
done
