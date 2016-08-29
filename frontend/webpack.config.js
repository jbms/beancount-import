const path = require('path');
var HtmlWebpackPlugin = require('html-webpack-plugin');
var HtmlWebpackInlineSourcePlugin = require('html-webpack-inline-source-plugin');

module.exports = (env, argv) => {
  const distDir = argv.mode === 'production' ? 'prod' : 'dev';
  const config = {
    entry : './app.tsx',
    devtool : argv.mode === 'production' ? false : 'cheap-module-source-map',
    output : {filename : '[name].bundle.js', path : path.resolve(__dirname, '../beancount_import/frontend_dist/' + distDir)},
    module : {
      rules : [
        {test : /\.tsx?$/, use : 'ts-loader'},
        {test : /\.css$/, use : [ 'style-loader', 'css-loader' ]},
      ],
    },
    resolve : {
      extensions : [ '.js', '.ts', '.tsx', '*' ],
    },
    plugins : [
      new HtmlWebpackPlugin({title : 'Beancount-import', inlineSource : '.(js|css)$'}),
      new HtmlWebpackInlineSourcePlugin(),
    ],
  };
  return config;
};
