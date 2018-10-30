package migration

type ErrorTransformer interface {
	Transform(Migration, error) error
}

type ErrorTransformerFn func(Migration, error) error

func (fn ErrorTransformerFn) Transform(m Migration, err error) error {
	return fn(m, err)
}

func defaultTransformerFn(_ Migration, err error) error { return err }

type multiTransformer []ErrorTransformer

func (vs multiTransformer) Transform(m Migration, err error) error {
	for _, v := range vs {
		if err := v.Transform(m, err); err != nil {
			return err
		}
	}

	return nil
}

func wrapTransformers(vs []ErrorTransformer) ErrorTransformer {
	switch len(vs) {
	case 0:
		return ErrorTransformerFn(defaultTransformerFn)
	case 1:
		return vs[0]
	}

	return multiTransformer(vs)
}
