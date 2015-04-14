/**
 * @author qiao / https://github.com/qiao
 * @author mrdoob / http://mrdoob.com
 * @author alteredq / http://alteredqualia.com/
 * @author WestLangley / http://github.com/WestLangley
 * @author aeharding / http://github.com/aeharding
 */

THREE.OrbitControls = function ( object, domElement ) {

  this.object = object;
  this.domElement = ( domElement !== undefined ) ? domElement : document;

  // API

  this.enabled = true;

  this.center = new THREE.Vector3();

  this.autoRotate = true;
  this.autoRotateSpeed = 0.05; // 30 seconds per round when fps is 60

  this.minPolarAngle = 0; // radians
  this.maxPolarAngle = Math.PI; // radians

  this.minDistance = 0;
  this.maxDistance = Infinity;

  // 65 /*A*/, 83 /*S*/, 68 /*D*/
  this.keys = { LEFT: 37, UP: 38, RIGHT: 39, BOTTOM: 40, ROTATE: 65, ZOOM: 83 };

  // internals

  var scope = this;

  var EPS = 0.000001;
  var PIXELS_PER_ROUND = 1800;

  var rotateStart = new THREE.Vector2();
  var rotateEnd = new THREE.Vector2();
  var rotateDelta = new THREE.Vector2();

  var zoomStart = new THREE.Vector2();
  var zoomEnd = new THREE.Vector2();
  var zoomDelta = new THREE.Vector2();

  var phiDelta = 0;
  var thetaDelta = 0;
  var scale = 1;

  var lastPosition = new THREE.Vector3();

  var STATE = { NONE: -1, ROTATE: 0 };
  var state = STATE.NONE;

  // events

  var changeEvent = { type: 'change' };


  this.rotateLeft = function ( angle ) {

    if ( angle === undefined ) {

      angle = getAutoRotationAngle();

    }

    thetaDelta -= angle;

  };

  this.rotateRight = function ( angle ) {

    if ( angle === undefined ) {

      angle = getAutoRotationAngle();

    }

    thetaDelta += angle;

  };

  this.rotateUp = function ( angle ) {

    if ( angle === undefined ) {

      angle = getAutoRotationAngle();

    }

    phiDelta -= angle;

  };

  this.rotateDown = function ( angle ) {

    if ( angle === undefined ) {

...
