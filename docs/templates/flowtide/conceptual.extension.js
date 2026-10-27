// Runs inside DocFX's template engine; no Node.js runtime is required.
exports.postTransform = function (model) {
  if (model.path && /\.md$/i.test(model.path)) {
    model._markdownHref = model._rel + model.path;
  }
  return model;
};
