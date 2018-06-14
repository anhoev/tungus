(function (exports) {
    "use strict";

    exports.stringify = function (obj) {

        return JSON.stringify(obj, function (key, value) {
            return value;
        });
    };

    exports.parse = function (str) {

        var iso8061 = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2}(?:\.\d*)?)Z$/;

        return JSON.parse(str, function (key, value) {
            if (typeof value != 'string') {
                return value;
            }
            if (value.length < 8) {
                return value;
            }

            if (iso8061 && value.match(iso8061)) {
                return new Date(value);
            }

            return value;
        });
    };

    exports.clone = function (obj, date2obj) {
        return exports.parse(exports.stringify(obj), date2obj);
    };

}(typeof exports === 'undefined' ? (window.JSONfn = {}) : exports));