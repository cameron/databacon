var gulp = require('gulp');
var watch = require('gulp-watch');
var shell = require('gulp-shell');
var sys = require('sys')
var exec = require('child_process').exec;

gulp.task('watch', function(){

  watch(['databacon/*py', 'tests/*py'], function(e){
    exec("pwd; PYTHON_PATH=. python tests/test.py", sys.puts);
  });
})

gulp.task('default', ['watch']);
