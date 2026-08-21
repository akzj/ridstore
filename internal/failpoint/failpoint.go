// Package failpoint provides explicit, dependency-injected fault boundaries.
// Production paths pass a nil Hook; there is no process-global activation.
package failpoint

type Point string

type Hook interface {
	Hit(Point) error
}

type Func func(Point) error

func (f Func) Hit(point Point) error { return f(point) }

func Hit(hook Hook, point Point) error {
	if hook == nil {
		return nil
	}
	return hook.Hit(point)
}
